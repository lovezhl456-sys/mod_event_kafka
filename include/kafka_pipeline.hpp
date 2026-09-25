#pragma once

#include "bounded_queue.hpp"
#include "kafka_outbox.hpp"

#include <librdkafka/rdkafka.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace event_kafka {

struct PipelineConfig {
  std::string brokers;
  std::string topic;
  std::string username;
  std::string password;
  std::string security_protocol;  // empty -> auto
  std::string ssl_ca_location;
  std::string compression{"snappy"};
  std::string outbox_path{"/tmp/event_kafka_outbox.db"};
  int buffer_size{100000};
  size_t mem_queue_max{10000};
  int64_t outbox_max_rows{100000};
  int64_t outbox_max_bytes{512LL * 1024 * 1024};
  int message_timeout_ms{30000};
  bool enable_idempotence{true};
  int worker_idle_ms{50};
  int poll_ms{100};
  int max_attempts_before_dead{50};
};

struct PipelineMetrics {
  std::atomic<uint64_t> enqueued{0};
  std::atomic<uint64_t> rejected_mem_full{0};
  std::atomic<uint64_t> rejected_disk_full{0};
  std::atomic<uint64_t> produce_ok{0};
  std::atomic<uint64_t> produce_fail{0};
  std::atomic<uint64_t> delivery_fail{0};
  std::atomic<uint64_t> acked{0};
  std::atomic<uint64_t> producer_rebuilds{0};
  std::atomic<uint64_t> permanent_errors{0};
};

class KafkaPipeline {
 public:
  explicit KafkaPipeline(PipelineConfig cfg);
  ~KafkaPipeline();

  bool start(std::string& err);
  void stop();

  bool enqueue(const std::string& payload, const std::string& msg_key,
               const std::string& call_uuid, std::string& event_id_out,
               std::string& err);

  PipelineMetrics& metrics() { return metrics_; }
  const PipelineMetrics& metrics() const { return metrics_; }
  OutboxStats outbox_stats() const;

 private:
  struct DrOpaque {
    std::string event_id;
  };

  static void on_delivery(rd_kafka_t* rk, const rd_kafka_message_t* msg,
                          void* opaque);
  bool create_producer_unlocked(std::string& err);
  void destroy_producer_unlocked();
  void rebuild_producer();
  void worker_loop();
  void poll_loop();
  bool produce_record(const OutboxRecord& rec, std::string& err);
  static bool is_permanent_error(rd_kafka_resp_err_t err);
  static int64_t backoff_ms(int attempts);

  PipelineConfig cfg_;
  PipelineMetrics metrics_;
  std::unique_ptr<BoundedQueue> queue_;
  std::unique_ptr<Outbox> outbox_;

  mutable std::mutex rk_mu_;
  rd_kafka_t* rk_{nullptr};
  rd_kafka_topic_t* rkt_{nullptr};

  std::atomic<bool> running_{false};
  std::atomic<bool> accept_{false};
  std::thread worker_;
  std::thread poller_;
};

}  // namespace event_kafka
