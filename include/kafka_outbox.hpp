#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace event_kafka {

enum class OutboxState { Pending, InFlight, Dead };

struct OutboxRecord {
  std::string event_id;
  std::string topic;
  std::string msg_key;
  std::string payload;
  OutboxState state{OutboxState::Pending};
  int attempts{0};
  int64_t next_attempt_at_ms{0};
  std::string last_error;
  int64_t created_at_ms{0};
  int64_t updated_at_ms{0};
  std::string call_uuid;
};

struct OutboxStats {
  int64_t pending{0};
  int64_t in_flight{0};
  int64_t dead{0};
  int64_t total_bytes{0};
};

class Outbox {
 public:
  Outbox(std::string db_path, int64_t max_rows, int64_t max_bytes);
  ~Outbox();
  Outbox(const Outbox&) = delete;
  Outbox& operator=(const Outbox&) = delete;

  bool open(std::string& err);
  void close();

  bool insert_pending(const OutboxRecord& rec, std::string& err);
  std::vector<OutboxRecord> fetch_due(int64_t now_ms, int limit);
  bool mark_in_flight(const std::string& event_id, std::string& err);
  bool mark_acked(const std::string& event_id, std::string& err);
  bool mark_retry(const std::string& event_id, int64_t next_ms,
                  const std::string& error, std::string& err);
  bool mark_dead(const std::string& event_id, const std::string& error,
                 std::string& err);
  bool requeue_in_flight(std::string& err);

  // Pending rows with (now_ms - created_at_ms) > ttl_ms become dead / expired_ttl.
  // ttl_ms <= 0 disables expiry and returns 0. Returns the number of rows
  // updated, or -1 on error. In-flight rows are left for ACK or retry.
  int expire_ttl(int64_t now_ms, int64_t ttl_ms, std::string& err);
  bool get(const std::string& event_id, OutboxRecord& out) const;

  OutboxStats stats() const;
  int64_t oldest_pending_age_ms(int64_t now) const;

 private:
  struct Impl;
  Impl* impl_;
};

std::string make_event_id();
int64_t wall_now_ms();

}  // namespace event_kafka
