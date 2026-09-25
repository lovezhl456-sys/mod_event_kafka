#include "kafka_pipeline.hpp"

#include <chrono>
#include <cstring>

namespace event_kafka {

KafkaPipeline::KafkaPipeline(PipelineConfig cfg) : cfg_(std::move(cfg)) {
  queue_ = std::make_unique<BoundedQueue>(cfg_.mem_queue_max);
  outbox_ = std::make_unique<Outbox>(cfg_.outbox_path, cfg_.outbox_max_rows, cfg_.outbox_max_bytes);
}

KafkaPipeline::~KafkaPipeline() { stop(); }

bool KafkaPipeline::start(std::string& err) {
  if (!outbox_->open(err)) return false;
  if (!outbox_->requeue_in_flight(err)) return false;
  {
    std::lock_guard<std::mutex> lk(rk_mu_);
    if (!create_producer_unlocked(err)) return false;
  }
  running_ = true;
  accept_ = true;
  worker_ = std::thread([this] { worker_loop(); });
  poller_ = std::thread([this] { poll_loop(); });
  return true;
}

void KafkaPipeline::stop() {
  if (!accept_.exchange(false) && !running_.load() && !worker_.joinable() &&
      !poller_.joinable()) {
    return;
  }
  accept_ = false;
  for (int i = 0; i < 100 && queue_ && queue_->size() > 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  running_ = false;
  if (queue_) queue_->notify_stop();
  if (worker_.joinable()) worker_.join();
  if (poller_.joinable()) poller_.join();
  {
    std::lock_guard<std::mutex> lk(rk_mu_);
    if (rk_) rd_kafka_flush(rk_, 5000);
    destroy_producer_unlocked();
  }
  if (outbox_) outbox_->close();
}

OutboxStats KafkaPipeline::outbox_stats() const {
  return outbox_ ? outbox_->stats() : OutboxStats{};
}

bool KafkaPipeline::enqueue(const std::string& payload, const std::string& msg_key,
                            const std::string& call_uuid, std::string& event_id_out,
                            std::string& err) {
  if (!accept_.load()) {
    err = "pipeline not accepting";
    return false;
  }
  QueuedEvent ev;
  ev.event_id = make_event_id();
  ev.topic = cfg_.topic;
  ev.msg_key = msg_key;
  ev.payload = payload;
  ev.call_uuid = call_uuid;
  ev.enqueued_at_ms = wall_now_ms();
  event_id_out = ev.event_id;
  if (!queue_->try_push(std::move(ev))) {
    metrics_.rejected_mem_full.fetch_add(1, std::memory_order_relaxed);
    err = "memory queue full";
    return false;
  }
  metrics_.enqueued.fetch_add(1, std::memory_order_relaxed);
  return true;
}

void KafkaPipeline::on_delivery(rd_kafka_t* rk, const rd_kafka_message_t* msg, void* /*opaque*/) {
  auto* pipe = static_cast<KafkaPipeline*>(rd_kafka_opaque(rk));
  auto* ctx = static_cast<DrOpaque*>(msg->_private);
  if (!pipe || !ctx) return;
  std::string e;
  if (msg->err) {
    pipe->metrics_.delivery_fail.fetch_add(1, std::memory_order_relaxed);
    if (msg->err == RD_KAFKA_RESP_ERR__FATAL) {
      pipe->rebuild_producer();
      pipe->outbox_->mark_retry(ctx->event_id, wall_now_ms() + backoff_ms(1),
                                rd_kafka_err2str(msg->err), e);
    } else if (is_permanent_error(msg->err)) {
      pipe->metrics_.permanent_errors.fetch_add(1, std::memory_order_relaxed);
      pipe->outbox_->mark_dead(ctx->event_id, rd_kafka_err2str(msg->err), e);
    } else {
      pipe->outbox_->mark_retry(ctx->event_id, wall_now_ms() + backoff_ms(1),
                                rd_kafka_err2str(msg->err), e);
    }
  } else {
    if (pipe->outbox_->mark_acked(ctx->event_id, e)) {
      pipe->metrics_.acked.fetch_add(1, std::memory_order_relaxed);
    }
  }
  delete ctx;
}

bool KafkaPipeline::create_producer_unlocked(std::string& err) {
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  if (rd_kafka_conf_set(conf, "bootstrap.servers", cfg_.brokers.c_str(), errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    err = errstr;
    rd_kafka_conf_destroy(conf);
    return false;
  }
  rd_kafka_conf_set(conf, "queue.buffering.max.messages",
                    std::to_string(cfg_.buffer_size).c_str(), errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "compression.codec", cfg_.compression.c_str(), errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "acks", "all", errstr, sizeof(errstr));
  if (cfg_.enable_idempotence) {
    rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr));
  }

  std::string proto = cfg_.security_protocol;
  if (proto.empty()) proto = cfg_.username.empty() ? "PLAINTEXT" : "SASL_PLAINTEXT";
  rd_kafka_conf_set(conf, "security.protocol", proto.c_str(), errstr, sizeof(errstr));
  if (!cfg_.username.empty()) {
    rd_kafka_conf_set(conf, "sasl.mechanisms", "PLAIN", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "sasl.username", cfg_.username.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "sasl.password", cfg_.password.c_str(), errstr, sizeof(errstr));
  }
  if (!cfg_.ssl_ca_location.empty()) {
    rd_kafka_conf_set(conf, "ssl.ca.location", cfg_.ssl_ca_location.c_str(), errstr,
                      sizeof(errstr));
  }

  rd_kafka_conf_set_opaque(conf, this);
  rd_kafka_conf_set_dr_msg_cb(conf, &KafkaPipeline::on_delivery);

  rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!rk) {
    err = errstr;
    return false;
  }

  rd_kafka_topic_conf_t* tconf = rd_kafka_topic_conf_new();
  if (rd_kafka_topic_conf_set(tconf, "message.timeout.ms",
                              std::to_string(cfg_.message_timeout_ms).c_str(), errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    err = errstr;
    rd_kafka_topic_conf_destroy(tconf);
    rd_kafka_destroy(rk);
    return false;
  }
  rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, cfg_.topic.c_str(), tconf);
  if (!rkt) {
    err = rd_kafka_err2str(rd_kafka_last_error());
    rd_kafka_destroy(rk);
    return false;
  }

  rk_ = rk;
  rkt_ = rkt;
  return true;
}

void KafkaPipeline::destroy_producer_unlocked() {
  if (rkt_) {
    rd_kafka_topic_destroy(rkt_);
    rkt_ = nullptr;
  }
  if (rk_) {
    rd_kafka_destroy(rk_);
    rk_ = nullptr;
  }
}

void KafkaPipeline::rebuild_producer() {
  metrics_.producer_rebuilds.fetch_add(1, std::memory_order_relaxed);
  std::string err;
  std::lock_guard<std::mutex> lk(rk_mu_);
  destroy_producer_unlocked();
  outbox_->requeue_in_flight(err);
  create_producer_unlocked(err);
}

void KafkaPipeline::worker_loop() {
  while (true) {
    const bool stopping = !running_.load();
    bool stop_flag = stopping;
    auto ev = queue_->pop_wait_for_ms(cfg_.worker_idle_ms, stop_flag);
    if (ev) {
      OutboxRecord rec;
      rec.event_id = ev->event_id;
      rec.topic = ev->topic.empty() ? cfg_.topic : ev->topic;
      rec.msg_key = ev->msg_key;
      rec.payload = ev->payload;
      rec.call_uuid = ev->call_uuid;
      rec.created_at_ms = ev->enqueued_at_ms ? ev->enqueued_at_ms : wall_now_ms();
      std::string err;
      if (!outbox_->insert_pending(rec, err)) {
        metrics_.rejected_disk_full.fetch_add(1, std::memory_order_relaxed);
      }
    }

    // Expire stale pending rows before the due scan so they are not produced.
    // Short outages within outbox_ttl_ms leave rows pending and follow the
    // existing heal-and-drain path. ttl <= 0 keeps that path with no expiry.
    const int64_t now_ms = wall_now_ms();
    if (cfg_.outbox_ttl_ms > 0) {
      std::string exp_err;
      const int expired = outbox_->expire_ttl(now_ms, cfg_.outbox_ttl_ms, exp_err);
      if (expired > 0) {
        metrics_.outbox_expired.fetch_add(static_cast<uint64_t>(expired), std::memory_order_relaxed);
      }
    }

    auto due = outbox_->fetch_due(now_ms, 32);
    for (const auto& rec : due) {
      std::string err;
      if (!outbox_->mark_in_flight(rec.event_id, err)) continue;
      if (!produce_record(rec, err)) {
        metrics_.produce_fail.fetch_add(1, std::memory_order_relaxed);
        if (err.rfind("FATAL:", 0) == 0) rebuild_producer();
        if (rec.attempts + 1 >= cfg_.max_attempts_before_dead) {
          outbox_->mark_dead(rec.event_id, err, err);
          metrics_.permanent_errors.fetch_add(1, std::memory_order_relaxed);
        } else {
          outbox_->mark_retry(rec.event_id, wall_now_ms() + backoff_ms(rec.attempts + 1), err,
                              err);
        }
      } else {
        metrics_.produce_ok.fetch_add(1, std::memory_order_relaxed);
      }
    }

    if (stopping && queue_->size() == 0 && outbox_->fetch_due(wall_now_ms(), 1).empty()) {
      break;
    }
  }
}

bool KafkaPipeline::produce_record(const OutboxRecord& rec, std::string& err) {
  std::lock_guard<std::mutex> lk(rk_mu_);
  if (!rk_ || !rkt_) {
    err = "producer not ready";
    return false;
  }
  auto* ctx = new DrOpaque{rec.event_id};
  rd_kafka_headers_t* hdrs = rd_kafka_headers_new(1);
  rd_kafka_header_add(hdrs, "x-fs-event-id", -1, rec.event_id.data(), rec.event_id.size());
  rd_kafka_resp_err_t perr = rd_kafka_producev(
      rk_, RD_KAFKA_V_RKT(rkt_), RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
      RD_KAFKA_V_VALUE(const_cast<char*>(rec.payload.data()), rec.payload.size()),
      RD_KAFKA_V_KEY(rec.msg_key.empty() ? nullptr : const_cast<char*>(rec.msg_key.data()),
                     rec.msg_key.size()),
      RD_KAFKA_V_HEADERS(hdrs), RD_KAFKA_V_OPAQUE(ctx), RD_KAFKA_V_END);
  if (perr) {
    err = rd_kafka_err2str(perr);
    rd_kafka_headers_destroy(hdrs);
    delete ctx;
    if (perr == RD_KAFKA_RESP_ERR__FATAL) err = std::string("FATAL:") + err;
    return false;
  }
  return true;
}

void KafkaPipeline::poll_loop() {
  while (running_.load()) {
    rd_kafka_t* rk = nullptr;
    {
      std::lock_guard<std::mutex> lk(rk_mu_);
      rk = rk_;
    }
    if (rk) rd_kafka_poll(rk, cfg_.poll_ms);
    else std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.poll_ms));
  }
  for (int i = 0; i < 50; ++i) {
    rd_kafka_t* rk = nullptr;
    {
      std::lock_guard<std::mutex> lk(rk_mu_);
      rk = rk_;
    }
    if (rk) rd_kafka_poll(rk, 50);
    else break;
  }
}

bool KafkaPipeline::is_permanent_error(rd_kafka_resp_err_t err) {
  switch (err) {
    case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_INVALID_MSG:
    case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
      return true;
    default:
      return false;
  }
}

int64_t KafkaPipeline::backoff_ms(int attempts) {
  int64_t base = 200;
  for (int i = 1; i < attempts && i < 8; ++i) base *= 2;
  if (base > 30000) base = 30000;
  return base + (wall_now_ms() % 97);
}

}  // namespace event_kafka
