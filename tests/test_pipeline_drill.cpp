// Real Kafka exercise for the candidate core; not a FreeSWITCH/legacy-module test.
#include "kafka_pipeline.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <sqlite3.h>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

static std::ofstream output(const fs::path& path) {
  std::ofstream f(path);
  f.exceptions(std::ios::badbit | std::ios::failbit);
  return f;
}

static std::string csv_text(const unsigned char* value) {
  std::string result = "\"";
  for (const char c : std::string(value ? reinterpret_cast<const char*>(value) : "")) {
    if (c == '\"') result += '\"';
    result += c;
  }
  return result + "\"";
}

static void final_snapshot(const fs::path& root, const std::string& path) {
  sqlite3* raw = nullptr;
  const int opened = sqlite3_open_v2(path.c_str(), &raw, SQLITE_OPEN_READONLY, nullptr);
  std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db(raw, sqlite3_close);
  if (opened != SQLITE_OK) throw std::runtime_error("cannot open final outbox snapshot");
  sqlite3_stmt* stmt = nullptr;
  const char* query = "SELECT event_id,state,last_error,created_at_ms,updated_at_ms FROM outbox";
  if (sqlite3_prepare_v2(raw, query, -1, &stmt, nullptr) != SQLITE_OK)
    throw std::runtime_error(sqlite3_errmsg(raw));
  std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(stmt, sqlite3_finalize);
  auto f = output(root / "outbox_final.csv");
  f << "event_id,state,last_error,created_at_ms,updated_at_ms\n";
  int status;
  while ((status = sqlite3_step(stmt)) == SQLITE_ROW) {
    f << csv_text(sqlite3_column_text(stmt, 0)) << ','
      << csv_text(sqlite3_column_text(stmt, 1)) << ','
      << csv_text(sqlite3_column_text(stmt, 2)) << ','
      << sqlite3_column_int64(stmt, 3) << ',' << sqlite3_column_int64(stmt, 4) << '\n';
  }
  if (status != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(raw));
}

static void configure(rd_kafka_conf_t* conf, const char* key, const std::string& value) {
  char error[512];
  if (rd_kafka_conf_set(conf, key, value.c_str(), error, sizeof(error)) != RD_KAFKA_CONF_OK)
    throw std::runtime_error(std::string(key) + ": " + error);
}

int main(int argc, char** argv) {
  if (argc != 7) {
    std::cerr << "usage: test_pipeline_drill RUN_DIR BROKERS TOPIC TTL_MS COMPRESSION MESSAGE_TIMEOUT_MS\n";
    return 2;
  }
  try {
    const fs::path root(argv[1]);
    const std::string start_id = event_kafka::make_event_id();
    output(root / "sender_before.json") << "{\"pid\":" << getpid()
      << ",\"start_id\":\"" << start_id << "\"}\n";
    output(root / "runtime.txt") << "scope=candidate-core\nlibrdkafka=" << rd_kafka_version_str()
      << "\ncompression=" << argv[5] << "\ntopic=" << argv[3] << "\n";
    auto injected = output(root / "injected_ids.txt");
    auto details = output(root / "injected_events.csv");
    auto rejected = output(root / "rejected_ids.txt");
    auto consumed = output(root / "consumed_ids.txt");
    auto records = output(root / "consumed_records.csv");
    details << "event_id,call_uuid,sequence,phase\n";
    records << "event_id,partition,offset,observed_at_ms\n";

    event_kafka::PipelineConfig cfg;
    cfg.brokers = argv[2]; cfg.topic = argv[3];
    cfg.outbox_path = (root / "outbox.db").string();
    cfg.outbox_ttl_ms = std::stoll(argv[4]);
    cfg.compression = argv[5]; cfg.message_timeout_ms = std::stoi(argv[6]);
    cfg.worker_idle_ms = 10; cfg.poll_ms = 20;
    event_kafka::KafkaPipeline pipe(cfg);
    std::string error;
    if (!pipe.start(error)) throw std::runtime_error(error);

    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    configure(conf, "bootstrap.servers", cfg.brokers);
    configure(conf, "group.id", "drill-" + start_id);
    configure(conf, "auto.offset.reset", "earliest");
    configure(conf, "enable.auto.commit", "false");
    char errstr[512];
    rd_kafka_t* consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) throw std::runtime_error(errstr);
    // The consumer is closed before producer shutdown on normal completion.
    std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> owner(consumer, rd_kafka_destroy);
    rd_kafka_poll_set_consumer(consumer);
    rd_kafka_topic_partition_list_t* subscription = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(subscription, cfg.topic.c_str(), RD_KAFKA_PARTITION_UA);
    const auto subscribe_error = rd_kafka_subscribe(consumer, subscription);
    rd_kafka_topic_partition_list_destroy(subscription);
    if (subscribe_error) throw std::runtime_error(rd_kafka_err2str(subscribe_error));

    int sequence = 0;
    bool ready = false, stopping = false;
    auto next = Clock::now();
    auto deadline = Clock::now() + std::chrono::minutes(20);  // orphan-run bound
    std::set<std::string> unique_consumed;
    while (Clock::now() < deadline) {
      if (!stopping && fs::exists(root / "stop")) {
        stopping = true;
        deadline = Clock::now() + std::chrono::seconds(30);
      }
      if (!stopping && Clock::now() >= next) {
        std::string phase;
        std::ifstream(root / "phase") >> phase;
        if (phase != "baseline" && phase != "during" && phase != "recovery")
          throw std::runtime_error("missing/invalid controller phase");
        std::string id, reason;
        const bool accepted = pipe.enqueue("{\"sequence\":" + std::to_string(sequence) + "}",
                                          "drill-call", "drill-call", id, reason);
        if (id.empty()) throw std::runtime_error("enqueue did not assign an ID: " + reason);
        injected << id << '\n';
        details << id << ",drill-call," << sequence++ << ',' << phase << '\n';
        if (!accepted) rejected << id << '\n';
        injected.flush(); details.flush(); rejected.flush();
        next = Clock::now() + std::chrono::milliseconds(100);
      }
      rd_kafka_message_t* msg = rd_kafka_consumer_poll(consumer, 25);
      if (msg) {
        if (!msg->err) {
          rd_kafka_headers_t* headers = nullptr;
          const void* value = nullptr; size_t length = 0;
          if (rd_kafka_message_headers(msg, &headers) ||
              rd_kafka_header_get_last(headers, "x-fs-event-id", &value, &length) || !value) {
            rd_kafka_message_destroy(msg);
            throw std::runtime_error("consumed record without x-fs-event-id");
          }
          const std::string id(static_cast<const char*>(value), length);
          consumed << id << '\n'; consumed.flush();
          records << id << ',' << msg->partition << ',' << msg->offset << ','
                  << event_kafka::wall_now_ms() << '\n'; records.flush();
          unique_consumed.insert(id);
        } else if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
          std::cerr << "CONSUMER_ERROR code=" << static_cast<int>(msg->err)
                    << ' ' << rd_kafka_message_errstr(msg) << '\n';
        }
        rd_kafka_message_destroy(msg);
      }
      if (!ready && unique_consumed.size() >= 5) {
        output(root / "ready") << "baseline consumed\n";
        ready = true;
      }
      const auto& metrics = pipe.metrics();
      if (stopping && metrics.acked + metrics.outbox_expired >= metrics.enqueued &&
          unique_consumed.size() >= metrics.acked.load()) break;
    }
    const auto& m = pipe.metrics();
    output(root / "metrics.json") << "{\"enqueued\":" << m.enqueued
      << ",\"acked\":" << m.acked << ",\"delivery_fail\":" << m.delivery_fail
      << ",\"produce_fail\":" << m.produce_fail << ",\"outbox_expired\":" << m.outbox_expired
      << ",\"producer_rebuilds\":" << m.producer_rebuilds << "}\n";
    rd_kafka_consumer_close(consumer);
    owner.reset();
    pipe.stop();
    final_snapshot(root, cfg.outbox_path);
    output(root / "sender_after.json") << "{\"pid\":" << getpid()
      << ",\"start_id\":\"" << start_id << "\"}\n";
    // Success here only means artifacts were written; the Python verifier gates PASS.
    return stopping && ready ? 0 : 4;
  } catch (const std::exception& exc) {
    std::cerr << "DRILL_ERROR " << exc.what() << '\n';
    return 3;
  }
}
