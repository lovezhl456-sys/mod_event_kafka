#include <sqlite3.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <iostream>
#include <set>
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

void scheduler_upgrade_and_fairness() {
  std::string path = base + "fairness.db", err;
  {
    Outbox b(path, 1000, 100000);
    REQUIRE(b.open(err));
    for (int c = 0; c < 96; c++)
      for (int n = 0; n < 2; n++)
        REQUIRE(b.insert_pending(row("fair-" + std::to_string(c), n), err));
  }
  sqlite3* db = nullptr;
  REQUIRE(sqlite3_open(path.c_str(), &db) == SQLITE_OK);
  REQUIRE(sqlite3_exec(db,
                       "DROP TABLE order_scheduler; PRAGMA user_version=2; UPDATE call_streams SET "
                       "last_served_ms=9000000000000000 WHERE call_uuid='fair-95'",
                       nullptr, nullptr, nullptr) == SQLITE_OK);
  sqlite3_close(db);
  Outbox b(path, 1000, 100000);
  REQUIRE(b.open(err));
  std::set<std::string> served;
  for (int round = 0; round < 3; round++) {
    auto due = b.fetch_due(wall_now_ms() + 1000, 32);
    REQUIRE(due.size() == 32);
    for (const auto& r : due) {
      REQUIRE(r.call_seq == 0);
      REQUIRE(served.insert(r.call_uuid).second);
      REQUIRE(b.mark_in_flight(r.event_id, err));
      REQUIRE(b.mark_acked(r.event_id, err));
    }
  }
  REQUIRE(served.size() == 96);
}
void concurrent_stop_admission() {
  PipelineConfig cfg;
  cfg.brokers = "127.0.0.1:1";
  cfg.topic = "stop";
  cfg.outbox_path = base + "stop-race.db";
  cfg.outbox_ttl_ms = 0;
  cfg.message_timeout_ms = 100;
  cfg.poll_ms = 5;
  cfg.worker_idle_ms = 5;
  cfg.require_source_sequence = true;
  KafkaPipeline p(cfg);
  std::string err;
  REQUIRE(p.start(err));
  std::atomic<int> accepted{0};
  std::vector<std::thread> senders;
  std::vector<std::vector<std::string>> ids(4);
  for (int c = 0; c < 4; c++)
    senders.emplace_back([&, c] {
      for (int n = 0; n < 20; n++) {
        std::string id, e;
        if (!p.enqueue("payload", "stop-" + std::to_string(c), "stop-" + std::to_string(c), id, e,
                       n))
          break;
        ids[c].push_back(id);
        ++accepted;
      }
    });
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (accepted < 4 && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
  auto began = std::chrono::steady_clock::now();
  p.stop();
  for (auto& t : senders) t.join();
  REQUIRE(std::chrono::steady_clock::now() - began < std::chrono::seconds(10));
  REQUIRE(accepted >= 4);
  Outbox b(cfg.outbox_path, 1000, 100000);
  REQUIRE(b.open(err));
  auto st = b.stats();
  REQUIRE(st.pending + st.in_flight == accepted);
  for (const auto& group : ids)
    for (const auto& id : group) {
      OutboxRecord r;
      REQUIRE(b.get(id, r));
    }
}

void admission_ordinals_and_reuse() {
  const auto path = base + "admission-reuse.db";
  std::string e;
  std::set<int64_t> ordinals;
  std::mutex mu;
  {
    Outbox b(path, 1000, 100000);
    REQUIRE(b.open(e));
    std::atomic<int> count{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < 8; ++t) ts.emplace_back([&, t] {
      for (int n = t; n < 64; n += 8) {
        auto r = row("same-call", n);
        r.call_seq = -1;
        r.created_at_ms -= n * 1000; // Reversed timestamps must not define order.
        int64_t assigned = -1;
        std::string err;
        if (b.insert_pending(r, err, &assigned)) {
          std::lock_guard<std::mutex> lock(mu);
          ordinals.insert(assigned);
          ++count;
        }
      }
    });
    for (auto& t : ts) t.join();
    REQUIRE(count == 64 && ordinals.size() == 64);
    for (int n = 0; n < 64; ++n) {
      REQUIRE(ordinals.count(n) == 1);
      auto due = b.fetch_due(wall_now_ms() + 10000, 100);
      REQUIRE(due.size() == 1 && due[0].call_seq == n);
      REQUIRE(b.mark_in_flight(due[0].event_id, e));
      REQUIRE(b.mark_acked(due[0].event_id, e));
    }
  }
  Outbox b(path, 1000, 100000);
  REQUIRE(b.open(e));
  auto late = row("same-call", 500); late.call_seq = -1;
  int64_t assigned = -1;
  REQUIRE(b.insert_pending(late, e, &assigned));
  REQUIRE(assigned == 64); // Idle/restart/reused ID never silently resets the cursor.
  REQUIRE(!b.insert_pending(late, e, &assigned));
  REQUIRE(assigned == -1); // Failed admission exposes no committed ordinal.
  auto after = row("same-call", 501); after.call_seq = -1;
  REQUIRE(b.insert_pending(after, e, &assigned));
  REQUIRE(assigned == 65); // Failed transaction did not consume a sequence.
}

void retired_fence_and_released_capacity() {
  const auto path = base + "retired.db";
  std::string e;
  {
    Outbox b(path, 1, 10000);
    REQUIRE(b.open(e));
    auto r = row("closed", 0); r.call_seq = -1;
    REQUIRE(b.insert_pending(r, e));
    REQUIRE(b.mark_in_flight(r.event_id, e)); REQUIRE(b.mark_acked(r.event_id, e));
    REQUIRE(!b.insert_pending(row("other", 0), e)); // active cursor cap despite empty queue
  }
  sqlite3* db = nullptr;
  REQUIRE(sqlite3_open(path.c_str(), &db) == SQLITE_OK);
  REQUIRE(sqlite3_exec(db, "BEGIN; INSERT INTO retired_calls SELECT topic,call_uuid,msg_key,"
                          "next_seq,1,'explicit test closure' FROM call_streams; DELETE FROM "
                          "call_streams; COMMIT;", nullptr,nullptr,nullptr) == SQLITE_OK);
  sqlite3_close(db);
  for (int restart = 0; restart < 2; ++restart) {
    Outbox b(path, 1, 10000);
    REQUIRE(b.open(e));
    auto reused = row("closed", 99); reused.call_seq = -1;
    REQUIRE(!b.insert_pending(reused, e));
    REQUIRE(e.find("retired") != std::string::npos);
    if (!restart) REQUIRE(b.insert_pending(row("other", 0), e));
    REQUIRE(b.fetch_due(wall_now_ms()+1000, 10).size() == 1);
  }
}

int64_t next_seq_of(const std::string& path, const std::string& call) {
  sqlite3* db = nullptr;
  REQUIRE(sqlite3_open(path.c_str(), &db) == SQLITE_OK);
  sqlite3_stmt* q = nullptr;
  REQUIRE(sqlite3_prepare_v2(db, "SELECT next_seq FROM call_streams WHERE call_uuid=?", -1, &q,
                             nullptr) == SQLITE_OK);
  sqlite3_bind_text(q, 1, call.c_str(), -1, SQLITE_TRANSIENT);
  REQUIRE(sqlite3_step(q) == SQLITE_ROW);
  const int64_t seq = sqlite3_column_int64(q, 0);
  sqlite3_finalize(q);
  sqlite3_close(db);
  return seq;
}

// Head in flight or in backoff past TTL must not kill successors. After the head is
// acknowledged, next_seq lands on a live row.
void ttl_does_not_stick_on_dead_successor() {
  const auto path = base + "ttl-successor.db";
  Outbox b(path, 20, 100000);
  std::string e;
  REQUIRE(b.open(e));
  const int64_t now = wall_now_ms();
  const int64_t ttl = 120000;
  auto head = row("C", 0), next = row("C", 1), later = row("C", 2);
  head.created_at_ms = next.created_at_ms = later.created_at_ms = now - ttl - 5000;
  REQUIRE(b.insert_pending(head, e));
  REQUIRE(b.insert_pending(next, e));
  REQUIRE(b.insert_pending(later, e));
  auto ungrouped = row("ignored", -1);
  ungrouped.call_uuid.clear();
  ungrouped.msg_key.clear();
  ungrouped.event_id = "free-old";
  ungrouped.created_at_ms = now - ttl - 100;
  REQUIRE(b.insert_pending(ungrouped, e));

  REQUIRE(b.expire_ttl(now, 0, e) == 0);
  REQUIRE(b.mark_in_flight(head.event_id, e));
  REQUIRE(b.expire_ttl(now, ttl, e) == 1);
  OutboxRecord got;
  REQUIRE(b.get(head.event_id, got));
  REQUIRE(got.state == OutboxState::InFlight);
  REQUIRE(b.get(next.event_id, got) && got.state == OutboxState::Pending);
  REQUIRE(b.get(later.event_id, got) && got.state == OutboxState::Pending);
  REQUIRE(b.get("free-old", got));
  REQUIRE(got.state == OutboxState::Dead && got.last_error == "expired_ttl");
  REQUIRE(next_seq_of(path, "C") == 0);
  REQUIRE(b.mark_acked(head.event_id, e));
  REQUIRE(b.expire_ttl(now, ttl, e) == 0);
  auto due = b.fetch_due(now + 1000, 10);
  REQUIRE(due.size() == 1 && due[0].event_id == next.event_id && due[0].call_seq == 1);

  auto blocked = row("D", 0), successor = row("D", 1);
  blocked.created_at_ms = successor.created_at_ms = now - ttl - 50;
  blocked.next_attempt_at_ms = now + 600000;
  REQUIRE(b.insert_pending(blocked, e));
  REQUIRE(b.insert_pending(successor, e));
  REQUIRE(b.expire_ttl(now, ttl, e) == 1);
  REQUIRE(b.get(blocked.event_id, got));
  REQUIRE(got.state == OutboxState::Dead && got.last_error == "expired_ttl");
  REQUIRE(b.get(successor.event_id, got) && got.state == OutboxState::Pending);
  REQUIRE(next_seq_of(path, "D") == 0);
  REQUIRE(b.fetch_due(now + 1000, 10).size() == 1);
  REQUIRE(b.retry_dead(blocked.event_id, e));
  REQUIRE(b.expire_ttl(now, ttl, e) == 1);
  REQUIRE(b.get(successor.event_id, got) && got.state == OutboxState::Pending);
  REQUIRE(b.retry_dead(blocked.event_id, e));
  REQUIRE(b.mark_in_flight(blocked.event_id, e));
  REQUIRE(b.mark_acked(blocked.event_id, e));
  REQUIRE(b.expire_ttl(now, ttl, e) == 0);
  due = b.fetch_due(now + 100000, 10);
  bool released = false;
  for (const auto& rec : due)
    if (rec.event_id == successor.event_id && rec.call_seq == 1) released = true;
  REQUIRE(released);
  REQUIRE(next_seq_of(path, "D") == 1);
}

void ttl_v4_upgrade_keeps_successor() {
  const auto path = base + "ttl-v4.db";
  sqlite3* raw = nullptr;
  REQUIRE(sqlite3_open(path.c_str(), &raw) == SQLITE_OK);
  const char* ddl =
      "CREATE TABLE outbox(event_id TEXT PRIMARY KEY,topic TEXT NOT NULL,msg_key TEXT,payload BLOB "
      "NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_attempt_at_ms INTEGER "
      "NOT NULL,last_error TEXT,created_at_ms INTEGER NOT NULL,updated_at_ms INTEGER NOT NULL,"
      "call_uuid TEXT,call_seq INTEGER);"
      "CREATE TABLE call_streams(topic TEXT NOT NULL,call_uuid TEXT NOT NULL,msg_key TEXT NOT NULL,"
      "mode TEXT NOT NULL,next_seq INTEGER NOT NULL DEFAULT 0,admit_seq INTEGER NOT NULL DEFAULT 0,"
      "last_served_ms INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(topic,call_uuid));"
      "PRAGMA user_version=4;"
      "INSERT INTO call_streams(topic,call_uuid,msg_key,mode,next_seq,admit_seq) "
      "VALUES('t','old','old','admission',0,2);"
      "INSERT INTO outbox VALUES('h','t','old','h','pending',0,1,'',1,1,'old',0);"
      "INSERT INTO outbox VALUES('s','t','old','s','pending',0,1,'',1,1,'old',1);";
  char* sql_err = nullptr;
  REQUIRE(sqlite3_exec(raw, ddl, nullptr, nullptr, &sql_err) == SQLITE_OK);
  sqlite3_free(sql_err);
  sqlite3_close(raw);
  Outbox b(path, 10, 100000);
  std::string e;
  REQUIRE(b.open(e));
  OutboxRecord got;
  REQUIRE(b.get("h", got) && got.call_seq == 0 && got.state == OutboxState::Pending);
  REQUIRE(b.get("s", got) && got.call_seq == 1 && got.state == OutboxState::Pending);
  REQUIRE(b.expire_ttl(wall_now_ms(), 10000, e) == 1);
  REQUIRE(b.get("h", got));
  REQUIRE(got.state == OutboxState::Dead && got.last_error == "expired_ttl" && got.call_seq == 0);
  REQUIRE(b.get("s", got) && got.state == OutboxState::Pending && got.call_seq == 1);
  REQUIRE(b.fetch_due(wall_now_ms() + 1000, 10).empty());
  REQUIRE(next_seq_of(path, "old") == 0);
  REQUIRE(b.retry_dead("h", e));
  REQUIRE(b.mark_in_flight("h", e));
  REQUIRE(b.mark_acked("h", e));
  REQUIRE(b.expire_ttl(wall_now_ms(), 10000, e) == 0);
  auto due = b.fetch_due(wall_now_ms() + 1000, 10);
  REQUIRE(due.size() == 1 && due[0].event_id == "s" && due[0].call_seq == 1);
}

void delivery_failure_reaches_dead_without_sticking_successor() {
  Outbox b(base + "delivery-policy.db", 20, 100000);
  std::string e;
  REQUIRE(b.open(e));
  auto head = row("P", 0), next = row("P", 1);
  REQUIRE(b.insert_pending(head, e));
  REQUIRE(b.insert_pending(next, e));
  const int limit = 3;
  for (int attempt = 1; attempt <= limit; ++attempt) {
    REQUIRE(b.mark_in_flight(head.event_id, e));
    OutboxRecord cur;
    REQUIRE(b.get(head.event_id, cur) && cur.attempts == attempt);
    const auto settled =
        settle_send_failure(b, limit, head.event_id, "broker timeout", false, e);
    if (attempt < limit) {
      REQUIRE(settled == SendFailureResult::Retry);
      auto due = b.fetch_due(wall_now_ms() + 600000, 10);
      REQUIRE(due.size() == 1 && due[0].event_id == head.event_id);
    } else {
      REQUIRE(settled == SendFailureResult::Dead);
    }
  }
  OutboxRecord got;
  REQUIRE(b.get(head.event_id, got) && got.state == OutboxState::Dead);
  REQUIRE(b.get(next.event_id, got) && got.state == OutboxState::Pending);
  REQUIRE(b.fetch_due(wall_now_ms() + 600000, 10).empty());
  REQUIRE(next_seq_of(base + "delivery-policy.db", "P") == 0);
  REQUIRE(b.retry_dead(head.event_id, e));
  REQUIRE(b.mark_in_flight(head.event_id, e));
  REQUIRE(b.mark_acked(head.event_id, e));
  auto due = b.fetch_due(wall_now_ms() + 1000, 10);
  REQUIRE(due.size() == 1 && due[0].event_id == next.event_id && due[0].call_seq == 1);
  REQUIRE(b.mark_in_flight(next.event_id, e));
  REQUIRE(b.mark_acked(next.event_id, e));

  auto poison = row("Q", 0), rest = row("Q", 1);
  REQUIRE(b.insert_pending(poison, e));
  REQUIRE(b.insert_pending(rest, e));
  REQUIRE(b.mark_in_flight(poison.event_id, e));
  REQUIRE(settle_send_failure(b, 50, poison.event_id, "topic authorization failed", true, e) ==
          SendFailureResult::Dead);
  REQUIRE(b.get(rest.event_id, got) && got.state == OutboxState::Pending);
  REQUIRE(b.mark_in_flight(poison.event_id, e) == false);
  REQUIRE(b.retry_dead(poison.event_id, e));
  REQUIRE(b.mark_in_flight(poison.event_id, e));
  REQUIRE(b.mark_acked(poison.event_id, e));
  due = b.fetch_due(wall_now_ms() + 1000, 10);
  REQUIRE(due.size() == 1 && due[0].call_seq == 1 && due[0].call_uuid == "Q");
}

void delivery_report_reaches_dead() {
  PipelineConfig cfg;
  cfg.brokers = "127.0.0.1:1";
  cfg.topic = "delivery-limit";
  cfg.outbox_path = base + "delivery-report.db";
  cfg.max_attempts_before_dead = 2;
  cfg.outbox_ttl_ms = 0;
  cfg.message_timeout_ms = 400;
  cfg.poll_ms = 20;
  cfg.worker_idle_ms = 10;
  cfg.enable_idempotence = false;
  KafkaPipeline pipe(cfg);
  std::string e, head, next;
  REQUIRE(pipe.start(e));
  REQUIRE(pipe.enqueue("head", "call", "call", head, e));
  REQUIRE(pipe.enqueue("next", "call", "call", next, e));
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  bool dead = false;
  while (std::chrono::steady_clock::now() < deadline) {
    sqlite3* db = nullptr;
    if (sqlite3_open_v2(cfg.outbox_path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK) {
      sqlite3_stmt* q = nullptr;
      if (sqlite3_prepare_v2(db, "SELECT state FROM outbox WHERE event_id=?", -1, &q, nullptr) ==
          SQLITE_OK) {
        sqlite3_bind_text(q, 1, head.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(q) == SQLITE_ROW) {
          const auto* state = sqlite3_column_text(q, 0);
          if (state && std::string(reinterpret_cast<const char*>(state)) == "dead") dead = true;
        }
      }
      sqlite3_finalize(q);
      sqlite3_close(db);
    }
    if (dead && pipe.metrics().delivery_fail.load() > 0) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
  }
  const auto delivery_fail = pipe.metrics().delivery_fail.load();
  const auto produce_fail = pipe.metrics().produce_fail.load();
  pipe.stop();
  if (!dead || delivery_fail == 0)
    throw std::runtime_error("delivery report did not reach dead; delivery_fail=" +
                             std::to_string(delivery_fail) +
                             " produce_fail=" + std::to_string(produce_fail));
  Outbox b(cfg.outbox_path, 20, 100000);
  REQUIRE(b.open(e));
  OutboxRecord got;
  REQUIRE(b.get(head, got) && got.state == OutboxState::Dead);
  REQUIRE(b.get(next, got) && got.state == OutboxState::Pending);
  REQUIRE(b.fetch_due(wall_now_ms() + 1000, 10).empty());
  REQUIRE(b.retry_dead(head, e));
  REQUIRE(b.mark_in_flight(head, e));
  REQUIRE(b.mark_acked(head, e));
  auto due = b.fetch_due(wall_now_ms() + 1000, 10);
  REQUIRE(due.size() == 1 && due[0].event_id == next && due[0].call_seq == 1);
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
    scheduler_upgrade_and_fairness();
    concurrent_stop_admission();
    admission_ordinals_and_reuse();
    retired_fence_and_released_capacity();
    ttl_does_not_stick_on_dead_successor();
    ttl_v4_upgrade_keeps_successor();
    delivery_failure_reaches_dead_without_sticking_successor();
    delivery_report_reaches_dead();
    fs::remove_all(base);
    std::cout << "ordering: "
                 "heads/backoff/inflight/dead/gaps/concurrency/restart/ownership/TTL/migration/"
                 "durable admission/successor TTL/delivery attempt limit PASS\n";
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
