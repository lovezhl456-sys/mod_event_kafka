#include "kafka_pipeline.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace event_kafka {

KafkaPipeline::KafkaPipeline(PipelineConfig cfg) : cfg_(std::move(cfg)) {
  outbox_ = std::make_unique<Outbox>(cfg_.outbox_path, cfg_.outbox_max_rows, cfg_.outbox_max_bytes);
}
KafkaPipeline::~KafkaPipeline() { stop(); }

bool KafkaPipeline::start(std::string& err) {
  std::lock_guard<std::mutex> lock(lifecycle_mu_);
  if (worker_.joinable() || running_) {
    err = "pipeline already started";
    return false;
  }
  if (!outbox_->open(err)) return false;
  if (!outbox_->requeue_in_flight(err) || !create_producer_unlocked(err)) {
    outbox_->close();
    return false;
  }
  running_ = true;
  accept_ = true;
  rebuild_requested_ = false;
  worker_ = std::thread([this] { worker_loop(); });
  return true;
}
void KafkaPipeline::stop() {
  std::lock_guard<std::mutex> lock(lifecycle_mu_);
  accept_ = false;
  running_ = false;
  if (worker_.joinable()) worker_.join();
  destroy_producer_unlocked();
  outbox_->close();
}
OutboxStats KafkaPipeline::outbox_stats() const { return outbox_->stats(); }

bool KafkaPipeline::enqueue(const std::string& payload, const std::string& msg_key,
                            const std::string& call_uuid, std::string& event_id_out,
                            std::string& err, int64_t source_sequence, int64_t* assigned_sequence) {
  if (assigned_sequence) *assigned_sequence = -1;
  std::lock_guard<std::mutex> lock(lifecycle_mu_);
  event_id_out.clear();
  if (!accept_) {
    err = "pipeline not accepting";
    return false;
  }
  if (source_sequence < -1 ||
      (cfg_.require_source_sequence && !call_uuid.empty() && source_sequence < 0)) {
    err = "contiguous per-call source sequence required (starts at zero)";
    return false;
  }
  OutboxRecord rec;
  rec.event_id = make_event_id();
  rec.topic = cfg_.topic;
  rec.msg_key = msg_key;
  rec.payload = payload;
  rec.call_uuid = call_uuid;
  rec.call_seq = source_sequence;
  rec.created_at_ms = wall_now_ms();
  event_id_out = rec.event_id;
  // Success means the event AND its call cursor are durably committed before returning.
  if (!outbox_->insert_pending(rec, err, assigned_sequence)) {
    metrics_.rejected_disk_full.fetch_add(1);
    return false;
  }
  metrics_.enqueued.fetch_add(1);
  return true;
}

void KafkaPipeline::on_delivery(rd_kafka_t* rk, const rd_kafka_message_t* msg, void*) {
  auto* pipe = static_cast<KafkaPipeline*>(rd_kafka_opaque(rk));
  std::unique_ptr<DrOpaque> ctx(static_cast<DrOpaque*>(msg->_private));
  if (!pipe || !ctx) return;
  std::string err;
  bool ok;
  if (msg->err) {
    pipe->metrics_.delivery_fail.fetch_add(1);
    if (msg->err == RD_KAFKA_RESP_ERR__FATAL) pipe->request_rebuild();
    const auto settled =
        settle_send_failure(*pipe->outbox_, pipe->cfg_.max_attempts_before_dead, ctx->event_id,
                            rd_kafka_err2str(msg->err), is_permanent_error(msg->err), err);
    ok = settled != SendFailureResult::Failed;
    if (settled == SendFailureResult::Dead) pipe->metrics_.permanent_errors.fetch_add(1);
  } else {
    ok = pipe->outbox_->mark_acked(ctx->event_id, err);
    if (ok) pipe->metrics_.acked.fetch_add(1);
  }
  if (!ok) {
    std::fprintf(stderr, "event_kafka delivery state failure: %s\n", err.c_str());
    // Do not proceed past an uncertain durable transition.
    pipe->metrics_.state_errors.fetch_add(1);
    pipe->accept_ = false;
    pipe->running_ = false;
  }
}

bool KafkaPipeline::create_producer_unlocked(std::string& err) {
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  auto set = [&](const char* key, const std::string& value) {
    if (rd_kafka_conf_set(conf, key, value.c_str(), errstr, sizeof(errstr)) == RD_KAFKA_CONF_OK)
      return true;
    err = std::string(key) + ": " + errstr;
    return false;
  };
  std::string proto = cfg_.security_protocol;
  if (proto.empty()) proto = cfg_.username.empty() ? "PLAINTEXT" : "SASL_PLAINTEXT";
  if (!set("bootstrap.servers", cfg_.brokers) ||
      !set("queue.buffering.max.messages", std::to_string(cfg_.buffer_size)) ||
      !set("compression.codec", cfg_.compression) || !set("acks", "all") ||
      !set("enable.idempotence", cfg_.enable_idempotence ? "true" : "false") ||
      !set("security.protocol", proto) ||
      (!cfg_.username.empty() &&
       (!set("sasl.mechanisms", "PLAIN") || !set("sasl.username", cfg_.username) ||
        !set("sasl.password", cfg_.password))) ||
      (!cfg_.ssl_ca_location.empty() && !set("ssl.ca.location", cfg_.ssl_ca_location))) {
    rd_kafka_conf_destroy(conf);
    return false;
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
  // Only worker_loop calls this, outside rd_kafka_poll/callback stack.
  if (rk_) {
    rd_kafka_purge(rk_, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
    rd_kafka_flush(rk_, 1000);
  }
  destroy_producer_unlocked();
  std::string err;
  if (!outbox_->requeue_in_flight(err) || !create_producer_unlocked(err))
    throw std::runtime_error("producer rebuild: " + err);
  metrics_.producer_rebuilds.fetch_add(1);
}

void KafkaPipeline::worker_loop() {
  try {
    while (running_) {
      if (rebuild_requested_.exchange(false)) rebuild_producer();
      if (!running_) break;
      const int64_t now = wall_now_ms();
      std::string err;
      int expired = outbox_->expire_ttl(now, cfg_.outbox_ttl_ms, err);
      if (expired < 0) throw std::runtime_error(err);
      metrics_.outbox_expired.fetch_add(expired);
      auto due = outbox_->fetch_due(now, 32);
      for (const auto& rec : due) {
        if (!running_ || rebuild_requested_) break;
        if (!outbox_->mark_in_flight(rec.event_id, err)) throw std::runtime_error(err);
        if (produce_record(rec, err)) {
          metrics_.produce_ok.fetch_add(1);
          continue;
        }
        metrics_.produce_fail.fetch_add(1);
        if (err.rfind("FATAL:", 0) == 0) request_rebuild();
        const bool permanent = err.rfind("PERMANENT:", 0) == 0;
        const std::string why = err;
        const auto settled = settle_send_failure(*outbox_, cfg_.max_attempts_before_dead,
                                                 rec.event_id, why, permanent, err);
        if (settled == SendFailureResult::Failed) throw std::runtime_error(err);
        if (settled == SendFailureResult::Dead) metrics_.permanent_errors.fetch_add(1);
      }
      // A single owner sends, polls, destroys and rebuilds the producer; no borrowed-rk race.
      if (rk_) rd_kafka_poll(rk_, cfg_.poll_ms);
      if (due.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.worker_idle_ms));
    }
  } catch (const std::exception& e) {
    std::fprintf(stderr, "event_kafka durable state failure: %s\n", e.what());
    metrics_.state_errors.fetch_add(1);
    accept_ = false;
    running_ = false;
  }
  if (rk_) {
    rd_kafka_flush(rk_, 1000);
    rd_kafka_purge(rk_, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
    rd_kafka_flush(rk_, 1000);
  }
}

bool KafkaPipeline::produce_record(const OutboxRecord& rec, std::string& err) {
  if (!rk_ || !rkt_) {
    err = "producer not ready";
    return false;
  }
  auto* ctx = new DrOpaque{rec.event_id};
  rd_kafka_headers_t* hdrs = rd_kafka_headers_new(2);
  if (rec.call_seq >= 0) {
    const auto seq = std::to_string(rec.call_seq);
    rd_kafka_header_add(hdrs, "x-fs-call-sequence", -1, seq.data(), seq.size());
  }
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
    if (perr == RD_KAFKA_RESP_ERR__FATAL)
      err = std::string("FATAL:") + err;
    else if (is_permanent_error(perr))
      err = std::string("PERMANENT:") + err;
    return false;
  }
  return true;
}

bool KafkaPipeline::is_permanent_error(rd_kafka_resp_err_t err) {
  switch (err) {
    case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_INVALID_MSG:
    case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
    case RD_KAFKA_RESP_ERR_INVALID_RECORD:
      return true;
    default:
      return false;
  }
}

int64_t attempt_backoff_ms(int attempts) {
  int64_t base = 200;
  for (int i = 1; i < attempts && i < 8; ++i) base *= 2;
  if (base > 30000) base = 30000;
  return base + (wall_now_ms() % 97);
}

SendFailureResult settle_send_failure(Outbox& outbox, int max_attempts, const std::string& event_id,
                                     const std::string& error, bool permanent, std::string& err) {
  OutboxRecord cur;
  if (!outbox.get(event_id, cur) || cur.state != OutboxState::InFlight) {
    err = "send failure for non-in-flight event";
    return SendFailureResult::Failed;
  }
  if (permanent || cur.attempts >= max_attempts) {
    if (!outbox.mark_dead(event_id, error, err)) return SendFailureResult::Failed;
    return SendFailureResult::Dead;
  }
  if (!outbox.mark_retry(event_id, wall_now_ms() + attempt_backoff_ms(cur.attempts), error, err))
    return SendFailureResult::Failed;
  return SendFailureResult::Retry;
}

}  // namespace event_kafka
