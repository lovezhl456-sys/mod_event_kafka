#include <sqlite3.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

#include "kafka_outbox.hpp"
#include "kafka_pipeline.hpp"
#define REQUIRE(x)                                                                             \
  do {                                                                                         \
    if (!(x)) throw std::runtime_error(std::string(#x) + " line " + std::to_string(__LINE__)); \
  } while (0)
using namespace event_kafka;
namespace fs = std::filesystem;
std::string base;
OutboxRecord row(std::string call, int64_t seq) {
  OutboxRecord r;
  r.event_id = call + "-" + std::to_string(seq);
  r.topic = "t";
  r.call_uuid = call;
  r.msg_key = call;
  r.call_seq = seq;
  r.payload = r.event_id;
  r.created_at_ms = wall_now_ms();
  return r;
}
void heads() {
  Outbox b(base + "heads.db", 100, 10000);
  std::string e;
  REQUIRE(b.open(e));
  auto a = row("A", 0), a1 = row("A", 1), other = row("B", 0);
  a.created_at_ms += 50;
  a1.created_at_ms -= 50;
  REQUIRE(b.insert_pending(a1, e));
  REQUIRE(b.insert_pending(a, e));
  REQUIRE(b.insert_pending(other, e));
  auto d = b.fetch_due(wall_now_ms() + 100, 32);
  REQUIRE(d.size() == 2);
  REQUIRE(!b.mark_in_flight(a1.event_id, e));
  REQUIRE(b.mark_in_flight(a.event_id, e));
  REQUIRE(b.fetch_due(wall_now_ms() + 100, 32).size() == 1);
  REQUIRE(b.mark_retry(a.event_id, wall_now_ms() + 5000, "timeout", e));
  REQUIRE(b.fetch_due(wall_now_ms(), 32).size() == 1);
  REQUIRE(b.mark_dead(a.event_id, "poison", e));
  REQUIRE(b.fetch_due(wall_now_ms() + 6000, 32).size() == 1);
  REQUIRE(!b.mark_acked(a.event_id, e));
  REQUIRE(b.retry_dead(a.event_id, e));
  REQUIRE(b.mark_in_flight(a.event_id, e));
  REQUIRE(b.mark_acked(a.event_id, e));
  REQUIRE(b.mark_in_flight(a1.event_id, e));
  REQUIRE(b.mark_acked(a1.event_id, e));
  REQUIRE(!b.insert_pending(a, e));
  auto changed = row("A", 2);
  changed.msg_key = "other-key";
  REQUIRE(!b.insert_pending(changed, e));
  changed.msg_key = "A";
  changed.call_seq = -1;
  REQUIRE(!b.insert_pending(changed, e));
  REQUIRE(b.mark_in_flight(other.event_id, e));
  REQUIRE(b.mark_acked(other.event_id, e));
  REQUIRE(b.stats().pending == 0);
}
void gaps_concurrency() {
  Outbox b(base + "gaps.db", 1000, 100000);
  std::string e;
  REQUIRE(b.open(e));
  auto two = row("C", 2);
  REQUIRE(b.insert_pending(two, e));
  REQUIRE(b.fetch_due(wall_now_ms() + 1000, 32).empty());
  auto zero = row("C", 0);
  REQUIRE(b.insert_pending(zero, e));
  REQUIRE(b.mark_in_flight(zero.event_id, e));
  REQUIRE(b.mark_acked(zero.event_id, e));
  REQUIRE(b.fetch_due(wall_now_ms() + 1000, 32).empty());
  auto one = row("C", 1);
  REQUIRE(b.insert_pending(one, e));
  REQUIRE(!b.insert_pending(one, e));
  REQUIRE(b.mark_in_flight(one.event_id, e));
  REQUIRE(b.mark_acked(one.event_id, e));
  REQUIRE(b.fetch_due(wall_now_ms() + 1000, 32)[0].call_seq == 2);
  std::atomic<int> count{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < 8; t++)
    ts.emplace_back([&, t] {
      for (int n = t; n < 64; n += 8) {
        auto r = row("parallel", n);
        std::string err;
        if (b.insert_pending(r, err)) ++count;
      }
    });
  for (auto& t : ts) t.join();
  REQUIRE(count == 64);
  for (int n = 0; n < 64; n++) {
    auto d = b.fetch_due(wall_now_ms() + 1000, 100);
    bool found = false;
    for (auto& r : d)
      if (r.call_uuid == "parallel") {
        REQUIRE(r.call_seq == n);
        REQUIRE(b.mark_in_flight(r.event_id, e));
        REQUIRE(b.mark_acked(r.event_id, e));
        found = true;
      }
    REQUIRE(found);
  }
}
void restart_and_ownership() {
  std::string path = base + "restart.db", e;
  {
    Outbox b(path, 100, 10000);
    REQUIRE(b.open(e));
    Outbox second(path, 100, 10000);
    REQUIRE(!second.open(e));
    auto a = row("R", 0), b1 = row("R", 1);
    REQUIRE(b.insert_pending(a, e));
    REQUIRE(b.insert_pending(b1, e));
    REQUIRE(b.mark_in_flight(a.event_id, e));
    REQUIRE(b.mark_acked(a.event_id, e));
    REQUIRE(b.mark_in_flight(b1.event_id, e));
  }
  Outbox b(path, 100, 10000);
  REQUIRE(b.open(e));
  REQUIRE(b.requeue_in_flight(e));
  auto d = b.fetch_due(wall_now_ms() + 1000, 32);
  REQUIRE(d.size() == 1 && d[0].call_seq == 1 && d[0].attempts == 1);
  REQUIRE(!b.insert_pending(row("R", 0), e));
  REQUIRE(b.insert_pending(row("R", 2), e));
  REQUIRE(d[0].payload == "R-1");
}
void ttl_barrier_capacity() {
  Outbox b(base + "ttl.db", 2, 10000);
  std::string e;
  REQUIRE(b.open(e));
  auto a = row("T", 0);
  a.created_at_ms = 1;
  REQUIRE(b.insert_pending(a, e));
  REQUIRE(b.insert_pending(row("T", 1), e));
  REQUIRE(b.expire_ttl(wall_now_ms(), 10000, e) == 1);
  REQUIRE(b.fetch_due(wall_now_ms() + 1000, 32).empty());
  REQUIRE(!b.insert_pending(row("B", 0), e));
  OutboxRecord got;
  REQUIRE(b.get(a.event_id, got));
  REQUIRE(got.state == OutboxState::Dead);
  REQUIRE(got.payload == a.payload && got.call_seq == 0);
}
void migration() {
  const char* ddl =
      "CREATE TABLE outbox(event_id TEXT PRIMARY KEY,topic TEXT NOT NULL,msg_key TEXT,payload BLOB "
      "NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at_ms INTEGER "
      "NOT NULL,last_error TEXT,created_at_ms INTEGER NOT NULL,updated_at_ms INTEGER NOT "
      "NULL,call_uuid TEXT);";
  for (int filled = 0; filled < 2; filled++) {
    std::string path = base + "legacy" + std::to_string(filled) + ".db", e;
    sqlite3* raw;
    REQUIRE(sqlite3_open(path.c_str(), &raw) == 0);
    REQUIRE(sqlite3_exec(raw, ddl, nullptr, nullptr, nullptr) == 0);
    if (filled)
      REQUIRE(sqlite3_exec(
                  raw,
                  "INSERT INTO outbox VALUES('old','t','k','payload','pending',0,1,'',1,1,'call')",
                  nullptr, nullptr, nullptr) == 0);
    sqlite3_close(raw);
    Outbox b(path, 100, 10000);
    if (filled) {
      REQUIRE(!b.open(e));
      REQUIRE(e.find("legacy nonempty") != std::string::npos);
    } else {
      REQUIRE(b.open(e));
      REQUIRE(b.insert_pending(row("new", 0), e));
    }
  }
}
void durability_admission() {
  PipelineConfig cfg;
  cfg.brokers = "127.0.0.1:1";
  cfg.topic = "t";
  cfg.outbox_path = base + "pipe.db";
  cfg.message_timeout_ms = 100;
  cfg.require_source_sequence = true;
  cfg.outbox_ttl_ms = 0;
  KafkaPipeline p(cfg);
  std::string e, id;
  REQUIRE(p.start(e));
  REQUIRE(!p.enqueue("missing", "call", "call", id, e));
  REQUIRE(p.enqueue("late", "call", "call", id, e, 1));
  REQUIRE(p.outbox_stats().pending == 1);
  REQUIRE(p.metrics().produce_ok == 0);
  p.stop();
  Outbox b(cfg.outbox_path, 100, 10000);
  REQUIRE(b.open(e));
  OutboxRecord got;
  REQUIRE(b.get(id, got));
  REQUIRE(got.call_seq == 1 && got.payload == "late");
  REQUIRE(b.fetch_due(wall_now_ms() + 1000, 32).empty());
}
void transactional_failures() {
  const std::string path = base + "transaction.db";
  Outbox box(path, 10, 10000);
  std::string err;
  REQUIRE(box.open(err));
  auto head = row("atomic", 0), next = row("atomic", 1);
  REQUIRE(box.insert_pending(head, err));
  REQUIRE(box.insert_pending(next, err));
  REQUIRE(box.mark_in_flight(head.event_id, err));
  sqlite3* raw = nullptr;
  REQUIRE(sqlite3_open(path.c_str(), &raw) == SQLITE_OK);
  REQUIRE(sqlite3_exec(raw,
                       "CREATE TRIGGER fail_delete BEFORE DELETE ON outbox BEGIN SELECT "
                       "RAISE(ABORT,'injected ACK storage failure'); END",
                       nullptr, nullptr, nullptr) == SQLITE_OK);
  REQUIRE(!box.mark_acked(head.event_id, err));
  REQUIRE(err.find("injected ACK storage failure") != std::string::npos);
  REQUIRE(box.fetch_due(wall_now_ms() + 1000, 32).empty());
  REQUIRE(sqlite3_exec(raw, "DROP TRIGGER fail_delete", nullptr, nullptr, nullptr) == SQLITE_OK);
  sqlite3_close(raw);
  REQUIRE(box.mark_acked(head.event_id,
                         err));  // cursor increment from failed transaction was rolled back
  REQUIRE(box.fetch_due(wall_now_ms() + 1000, 32)[0].call_seq == 1);

  auto a = row("admission", -1);
  a.event_id = "admission-0";
  REQUIRE(box.insert_pending(a, err));
  REQUIRE(
      !box.insert_pending(a, err));  // failed INSERT must not consume the next admission ordinal
  a.event_id = "admission-1";
  REQUIRE(box.insert_pending(a, err));
  OutboxRecord got;
  REQUIRE(box.get(a.event_id, got));
  REQUIRE(got.call_seq == 1);
}

int main() {
  try {
    base = "/tmp/event-kafka-order-" + std::to_string(getpid()) + "/";
    fs::create_directories(base);
    heads();
    gaps_concurrency();
    restart_and_ownership();
    ttl_barrier_capacity();
    migration();
    durability_admission();
    transactional_failures();
    fs::remove_all(base);
    std::cout << "ordering: "
                 "heads/backoff/inflight/dead/gaps/concurrency/restart/ownership/TTL/migration/"
                 "durable admission PASS\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
