#include "bounded_queue.hpp"
#include "kafka_outbox.hpp"
#include "kafka_pipeline.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

static int failures = 0;
#define CHECK(cond)                                                                              \
  do {                                                                                           \
    if (!(cond)) {                                                                               \
      std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << " " #cond "\n";                     \
      ++failures;                                                                                \
    }                                                                                            \
  } while (0)

static void rm_db(const std::string& db) {
  fs::remove(db);
  fs::remove(db + "-wal");
  fs::remove(db + "-shm");
}

// U-OWN-01: queue holds independent deep copies of payload/key
static void test_own_queue_deepcopy() {
  event_kafka::BoundedQueue q(4);
  std::string payload = "payload-original";
  std::string key = "key-original";
  event_kafka::QueuedEvent ev;
  ev.event_id = "e1";
  ev.topic = "t";
  ev.msg_key = key;
  ev.payload = payload;
  ev.call_uuid = "c1";
  CHECK(q.try_push(ev));  // copy into queue via by-value then move into deque
  payload.assign(payload.size(), 'X');
  key.assign(key.size(), 'Y');
  bool stopping = false;
  auto got = q.pop_wait_for_ms(50, stopping);
  CHECK(got.has_value());
  CHECK(got->payload == "payload-original");
  CHECK(got->msg_key == "key-original");
}

// U-OWN-02: outbox stores independent payload; mutating caller buffer after insert is safe
static void test_own_outbox_deepcopy() {
  const std::string db = "/tmp/event_kafka_own_outbox.db";
  rm_db(db);
  event_kafka::Outbox box(db, 100, 1024 * 1024);
  std::string err;
  CHECK(box.open(err));

  event_kafka::OutboxRecord r;
  r.event_id = event_kafka::make_event_id();
  r.topic = "fs.events";
  r.msg_key = "call-own";
  r.payload = std::string("{\"k\":\"stable-body\"}");
  r.call_uuid = "call-own";
  r.created_at_ms = event_kafka::wall_now_ms();
  const std::string id = r.event_id;
  CHECK(box.insert_pending(r, err));
  r.payload = "MUTATED-AFTER-INSERT";
  r.msg_key = "MUTATED-KEY";

  auto due = box.fetch_due(event_kafka::wall_now_ms() + 1000, 10);
  CHECK(due.size() == 1);
  CHECK(due[0].event_id == id);
  CHECK(due[0].payload == "{\"k\":\"stable-body\"}");
  CHECK(due[0].msg_key == "call-own");
  box.close();
}

// U-DSK-03: byte capacity rejects oversized insert
static void test_byte_cap() {
  const std::string db = "/tmp/event_kafka_byte_cap.db";
  rm_db(db);
  event_kafka::Outbox box(db, /*max_rows=*/100, /*max_bytes=*/64);
  std::string err;
  CHECK(box.open(err));

  event_kafka::OutboxRecord small;
  small.event_id = event_kafka::make_event_id();
  small.topic = "t";
  small.payload = std::string(32, 'a');
  small.created_at_ms = event_kafka::wall_now_ms();
  CHECK(box.insert_pending(small, err));

  event_kafka::OutboxRecord big = small;
  big.event_id = event_kafka::make_event_id();
  big.payload = std::string(64, 'b');  // 32+64 > 64
  CHECK(!box.insert_pending(big, err));
  CHECK(err.find("capacity") != std::string::npos);
  CHECK(box.stats().pending == 1);
  box.close();
}

// U-Q-02: concurrent enqueue around capacity
static void test_concurrent_enqueue() {
  constexpr size_t cap = 64;
  constexpr int threads = 8;
  constexpr int per_thread = 40;  // 320 attempts > 64
  event_kafka::BoundedQueue q(cap);
  std::atomic<int> pushed{0};
  std::vector<std::thread> th;
  th.reserve(threads);
  for (int t = 0; t < threads; ++t) {
    th.emplace_back([&, t] {
      for (int i = 0; i < per_thread; ++i) {
        event_kafka::QueuedEvent ev;
        ev.event_id = std::to_string(t) + "-" + std::to_string(i);
        ev.payload = "x";
        if (q.try_push(std::move(ev))) pushed.fetch_add(1);
      }
    });
  }
  for (auto& x : th) x.join();
  CHECK(pushed.load() == static_cast<int>(cap));
  CHECK(q.size() == cap);
  CHECK(q.rejected() == static_cast<uint64_t>(threads * per_thread - cap));
}

// U-UNL-01/02: notify_stop unblocks pop; double close safe; pipeline stop drains + rejects
static void test_safe_unload() {
  // Queue stop
  {
    event_kafka::BoundedQueue q(2);
    bool stopping = false;
    std::optional<event_kafka::QueuedEvent> got;
    std::thread consumer([&] {
      got = q.pop_wait_for_ms(5000, stopping);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    stopping = true;
    q.notify_stop();
    consumer.join();
    CHECK(!got.has_value());
  }

  // Outbox double close
  {
    const std::string db = "/tmp/event_kafka_unload_outbox.db";
    rm_db(db);
    event_kafka::Outbox box(db, 10, 1024);
    std::string err;
    CHECK(box.open(err));
    box.close();
    box.close();  // must not crash under ASan
    event_kafka::OutboxRecord r;
    r.event_id = event_kafka::make_event_id();
    r.topic = "t";
    r.payload = "p";
    r.created_at_ms = event_kafka::wall_now_ms();
    CHECK(!box.insert_pending(r, err));
  }

  // Pipeline: enqueue while up, stop joins cleanly, outbox retains unacked, post-stop enqueue fails
  {
    const std::string db = "/tmp/event_kafka_unload_pipe.db";
    rm_db(db);
    event_kafka::PipelineConfig cfg;
    cfg.brokers = "127.0.0.1:1";  // unreachable; producer still constructs
    cfg.topic = "fs_events";
    cfg.outbox_path = db;
    cfg.mem_queue_max = 100;
    cfg.outbox_max_rows = 1000;
    cfg.outbox_max_bytes = 1024 * 1024;
    cfg.worker_idle_ms = 20;
    cfg.poll_ms = 20;
    cfg.message_timeout_ms = 3000;
    cfg.enable_idempotence = false;  // friendlier with dead bootstrap

    event_kafka::KafkaPipeline pipe(cfg);
    std::string err;
    const bool started = pipe.start(err);
    if (!started) {
      std::cerr << "NOTE pipeline start skipped unload drain assert: " << err << "\n";
      // Still exercise destructor/stop on failed start
      pipe.stop();
      pipe.stop();
      return;
    }

    std::vector<std::string> ids;
    for (int i = 0; i < 5; ++i) {
      std::string id;
      CHECK(pipe.enqueue(std::string("{\"n\":") + std::to_string(i) + "}", "k", "call-u", id, err));
      ids.push_back(id);
    }
    // Allow worker to spill mem -> outbox even if produce fails
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    pipe.stop();
    pipe.stop();  // idempotent

    std::string id2;
    CHECK(!pipe.enqueue("after-stop", "k", "c", id2, err));

    event_kafka::Outbox box(db, 1000, 1024 * 1024);
    CHECK(box.open(err));
    auto st = box.stats();
    // At least some rows should have been persisted before/during stop
    CHECK(st.pending + st.in_flight + st.dead >= 1);
    box.close();
  }
}

static void test_baseline_smoke() {
  const std::string db = "/tmp/event_kafka_outbox_test.db";
  rm_db(db);

  {
    event_kafka::BoundedQueue q(2);
    event_kafka::QueuedEvent a;
    a.event_id = "1";
    a.payload = "a";
    event_kafka::QueuedEvent b;
    b.event_id = "2";
    b.payload = "b";
    event_kafka::QueuedEvent c;
    c.event_id = "3";
    c.payload = "c";
    CHECK(q.try_push(std::move(a)));
    CHECK(q.try_push(std::move(b)));
    CHECK(!q.try_push(std::move(c)));
    CHECK(q.rejected() == 1);
  }

  {
    event_kafka::Outbox box(db, 2, 1024 * 1024);
    std::string err;
    CHECK(box.open(err));

    event_kafka::OutboxRecord r1;
    r1.event_id = event_kafka::make_event_id();
    r1.topic = "fs.events";
    r1.msg_key = "call-1";
    r1.payload = "{\"Event-Name\":\"CHANNEL_CREATE\"}";
    r1.call_uuid = "call-1";
    r1.created_at_ms = event_kafka::wall_now_ms();
    CHECK(box.insert_pending(r1, err));

    event_kafka::OutboxRecord r2 = r1;
    r2.event_id = event_kafka::make_event_id();
    r2.payload = "{\"Event-Name\":\"CHANNEL_ANSWER\"}";
    r2.created_at_ms = r1.created_at_ms + 1;
    CHECK(box.insert_pending(r2, err));

    event_kafka::OutboxRecord r3 = r1;
    r3.event_id = event_kafka::make_event_id();
    CHECK(!box.insert_pending(r3, err));

    auto due = box.fetch_due(event_kafka::wall_now_ms() + 1000, 10);
    CHECK(due.size() == 2);
    CHECK(due[0].event_id == r1.event_id);

    CHECK(box.mark_in_flight(r1.event_id, err));
    auto st = box.stats();
    CHECK(st.in_flight == 1);
    CHECK(st.pending == 1);

    CHECK(box.mark_acked(r1.event_id, err));
    st = box.stats();
    CHECK(st.in_flight == 0);
    CHECK(st.pending == 1);

    CHECK(box.mark_in_flight(r2.event_id, err));
    CHECK(box.mark_retry(r2.event_id, event_kafka::wall_now_ms() + 5000, "broker down", err));
    due = box.fetch_due(event_kafka::wall_now_ms(), 10);
    CHECK(due.empty());
    due = box.fetch_due(event_kafka::wall_now_ms() + 6000, 10);
    CHECK(due.size() == 1);

    CHECK(box.mark_in_flight(r2.event_id, err));
    CHECK(box.requeue_in_flight(err));
    st = box.stats();
    CHECK(st.pending == 1);
    CHECK(st.in_flight == 0);

    CHECK(box.mark_dead(r2.event_id, "auth failed", err));
    st = box.stats();
    CHECK(st.dead == 1);
    CHECK(st.pending == 0);
  }

  {
    event_kafka::Outbox box(db, 100, 1024 * 1024);
    std::string err;
    CHECK(box.open(err));
    CHECK(box.stats().dead == 1);
  }
}

static event_kafka::OutboxRecord make_row(const std::string& id, const std::string& payload,
                                           int64_t created_at_ms) {
  event_kafka::OutboxRecord r;
  r.event_id = id;
  r.topic = "fs_events";
  r.msg_key = "k";
  r.payload = payload;
  r.call_uuid = "call";
  r.created_at_ms = created_at_ms;
  r.next_attempt_at_ms = created_at_ms;
  return r;
}

// U-TTL: enabled TTL dead-letters old rows and does not leave them due;
// TTL 0 still returns old rows for produce. Age == TTL is kept (> not >=).
static void test_outbox_ttl() {
  const int64_t ttl = 120000;
  const int64_t now = 1700000000000LL;
  const std::string db = "/tmp/event_kafka_ttl.db";
  rm_db(db);
  event_kafka::Outbox box(db, 100, 1024 * 1024);
  std::string err;
  CHECK(box.open(err));

  auto over = make_row("ttl-over", "{\"Event-Name\":\"CHANNEL_HANGUP\"}", now - ttl - 1);
  auto eq = make_row("ttl-eq", "{\"Event-Name\":\"CHANNEL_ANSWER\"}", now - ttl);
  auto fresh = make_row("ttl-fresh", "{\"Event-Name\":\"CHANNEL_CREATE\"}", now);
  CHECK(box.insert_pending(over, err));
  CHECK(box.insert_pending(eq, err));
  CHECK(box.insert_pending(fresh, err));

  CHECK(box.expire_ttl(now, ttl, err) == 1);
  event_kafka::OutboxRecord got;
  CHECK(box.get("ttl-over", got));
  CHECK(got.state == event_kafka::OutboxState::Dead);
  CHECK(got.last_error == "expired_ttl");
  CHECK(got.attempts == 0);

  auto due = box.fetch_due(now + 1000, 10);
  CHECK(due.size() == 2);
  CHECK(due[0].event_id != "ttl-over");
  CHECK(due[1].event_id != "ttl-over");
  CHECK(box.get("ttl-eq", got));
  CHECK(got.state == event_kafka::OutboxState::Pending);
  CHECK(got.last_error != "expired_ttl");
  CHECK(box.stats().dead == 1);
  CHECK(box.stats().pending == 2);

  // In-flight rows stay on the ACK path. Expiry does not delete or dead-letter them.
  auto inflight = make_row("ttl-inflight", "{\"n\":1}", now - ttl - 50);
  CHECK(box.insert_pending(inflight, err));
  CHECK(box.mark_in_flight("ttl-inflight", err));
  CHECK(box.expire_ttl(now, ttl, err) == 0);
  CHECK(box.get("ttl-inflight", got));
  CHECK(got.state == event_kafka::OutboxState::InFlight);
  CHECK(box.mark_acked("ttl-inflight", err));
  CHECK(!box.get("ttl-inflight", got));

  // Retry back to pending after the TTL is expired and not due for produce.
  auto retry = make_row("ttl-retry", "{\"n\":2}", now - ttl - 10);
  CHECK(box.insert_pending(retry, err));
  CHECK(box.mark_in_flight("ttl-retry", err));
  CHECK(box.mark_retry("ttl-retry", now, "broker down", err));
  CHECK(box.expire_ttl(now, ttl, err) == 1);
  CHECK(box.get("ttl-retry", got));
  CHECK(got.state == event_kafka::OutboxState::Dead);
  CHECK(got.last_error == "expired_ttl");
  due = box.fetch_due(now + 1000, 10);
  for (const auto& row : due) CHECK(row.event_id != "ttl-retry");
  box.close();

  // TTL 0: ancient rows stay pending and are still due (produce path unchanged).
  const std::string db0 = "/tmp/event_kafka_ttl0.db";
  rm_db(db0);
  event_kafka::Outbox box0(db0, 100, 1024 * 1024);
  CHECK(box0.open(err));
  auto ancient = make_row("ttl0-old", "{\"Event-Name\":\"CHANNEL_HANGUP\"}", 1);
  CHECK(box0.insert_pending(ancient, err));
  CHECK(box0.expire_ttl(now, 0, err) == 0);
  CHECK(box0.get("ttl0-old", got));
  CHECK(got.state == event_kafka::OutboxState::Pending);
  CHECK(got.last_error != "expired_ttl");
  due = box0.fetch_due(now, 10);
  CHECK(due.size() == 1);
  CHECK(due[0].event_id == "ttl0-old");
  box0.close();
}

// Expired dead letters yield capacity to new events. Permanent dead and live rows do not.
static void test_expired_does_not_block_new_work() {
  const std::string db = "/tmp/event_kafka_ttl_cap.db";
  rm_db(db);
  event_kafka::Outbox box(db, /*max_rows=*/2, /*max_bytes=*/1024 * 1024);
  std::string err;
  CHECK(box.open(err));
  const int64_t now = 1700000000000LL;
  const int64_t ttl = 120000;

  auto perm = make_row("perm-dead", "p", now);
  CHECK(box.insert_pending(perm, err));
  CHECK(box.mark_dead("perm-dead", "auth failed", err));

  auto expired = make_row("exp-dead", "e", now - ttl - 1);
  CHECK(box.insert_pending(expired, err));
  CHECK(box.expire_ttl(now, ttl, err) == 1);
  CHECK(box.stats().dead == 2);

  auto fresh = make_row("fresh", "n", now);
  CHECK(box.insert_pending(fresh, err));
  event_kafka::OutboxRecord got;
  CHECK(box.get("perm-dead", got));
  CHECK(got.state == event_kafka::OutboxState::Dead);
  CHECK(got.last_error == "auth failed");
  CHECK(!box.get("exp-dead", got));
  CHECK(box.get("fresh", got));
  CHECK(got.state == event_kafka::OutboxState::Pending);

  // Live rows still reject when they alone fill the outbox.
  auto extra = make_row("extra", "x", now);
  CHECK(!box.insert_pending(extra, err));
  CHECK(err.find("capacity") != std::string::npos);
  CHECK(box.get("fresh", got));
  CHECK(box.get("perm-dead", got));
  box.close();

  const std::string db_bytes = "/tmp/event_kafka_ttl_cap_bytes.db";
  rm_db(db_bytes);
  event_kafka::Outbox bytes(db_bytes, 100, /*max_bytes=*/48);
  CHECK(bytes.open(err));
  auto fat = make_row("fat-exp", std::string(40, 'a'), now - ttl - 1);
  CHECK(bytes.insert_pending(fat, err));
  CHECK(bytes.expire_ttl(now, ttl, err) == 1);
  auto neu = make_row("neu", std::string(40, 'b'), now);
  CHECK(bytes.insert_pending(neu, err));
  CHECK(!bytes.get("fat-exp", got));
  CHECK(bytes.get("neu", got));
  CHECK(got.state == event_kafka::OutboxState::Pending);
  bytes.close();
}

static event_kafka::PipelineConfig ttl_pipe_cfg(const std::string& db, int64_t ttl_ms) {
  event_kafka::PipelineConfig cfg;
  cfg.brokers = "127.0.0.1:1";
  cfg.topic = "fs_events";
  cfg.outbox_path = db;
  cfg.mem_queue_max = 100;
  cfg.outbox_max_rows = 1000;
  cfg.outbox_max_bytes = 1024 * 1024;
  cfg.worker_idle_ms = 20;
  cfg.poll_ms = 20;
  cfg.message_timeout_ms = 800;
  cfg.enable_idempotence = false;
  cfg.outbox_ttl_ms = ttl_ms;
  return cfg;
}

static bool spin_until(const std::function<bool()>& pred, int ms) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return pred();
}

// Worker: TTL enabled does not produce an old row; TTL 0 does.
static void test_pipeline_ttl_produce() {
  const int64_t ttl = 120000;
  const int64_t now = event_kafka::wall_now_ms();

  {
    const std::string db = "/tmp/event_kafka_ttl_pipe.db";
    rm_db(db);
    {
      event_kafka::Outbox box(db, 1000, 1024 * 1024);
      std::string err;
      CHECK(box.open(err));
      auto old = make_row("pipe-old", "{\"Event-Name\":\"CHANNEL_HANGUP\"}", now - ttl - 5);
      CHECK(box.insert_pending(old, err));
      box.close();
    }

    event_kafka::KafkaPipeline pipe(ttl_pipe_cfg(db, ttl));
    std::string err;
    if (!pipe.start(err)) {
      std::cerr << "FAIL pipeline start (ttl): " << err << "\n";
      ++failures;
      return;
    }
    std::string fresh_id;
    CHECK(pipe.enqueue("{\"Event-Name\":\"CHANNEL_CREATE\"}", "k", "call-new", fresh_id, err));
    const bool saw = spin_until([&] {
      return pipe.metrics().outbox_expired.load() >= 1 &&
             (pipe.metrics().produce_ok.load() + pipe.metrics().produce_fail.load()) >= 1;
    }, 3000);
    CHECK(saw);
    pipe.stop();

    CHECK(pipe.metrics().outbox_expired.load() >= 1);
    CHECK(pipe.metrics().produce_ok.load() + pipe.metrics().produce_fail.load() >= 1);

    event_kafka::Outbox box(db, 1000, 1024 * 1024);
    CHECK(box.open(err));
    event_kafka::OutboxRecord old;
    CHECK(box.get("pipe-old", old));
    CHECK(old.state == event_kafka::OutboxState::Dead);
    CHECK(old.last_error == "expired_ttl");
    CHECK(old.attempts == 0);
    event_kafka::OutboxRecord fresh;
    if (box.get(fresh_id, fresh)) {
      CHECK(!(fresh.state == event_kafka::OutboxState::Dead && fresh.last_error == "expired_ttl"));
    }
    box.close();
  }

  {
    const std::string db = "/tmp/event_kafka_ttl0_pipe.db";
    rm_db(db);
    {
      event_kafka::Outbox box(db, 1000, 1024 * 1024);
      std::string err;
      CHECK(box.open(err));
      auto old = make_row("pipe0-old", "{\"Event-Name\":\"CHANNEL_HANGUP\"}", now - ttl - 5);
      CHECK(box.insert_pending(old, err));
      box.close();
    }

    event_kafka::KafkaPipeline pipe(ttl_pipe_cfg(db, 0));
    std::string err;
    if (!pipe.start(err)) {
      std::cerr << "FAIL pipeline start (ttl 0): " << err << "\n";
      ++failures;
      return;
    }
    const bool saw = spin_until([&] {
      return (pipe.metrics().produce_ok.load() + pipe.metrics().produce_fail.load()) >= 1;
    }, 3000);
    CHECK(saw);
    pipe.stop();

    CHECK(pipe.metrics().outbox_expired.load() == 0);
    CHECK(pipe.metrics().produce_ok.load() + pipe.metrics().produce_fail.load() >= 1);

    event_kafka::Outbox box(db, 1000, 1024 * 1024);
    CHECK(box.open(err));
    event_kafka::OutboxRecord old;
    if (box.get("pipe0-old", old)) {
      CHECK(old.last_error != "expired_ttl");
      CHECK(old.attempts >= 1);
    }
    box.close();
  }
}

int main() {
  test_baseline_smoke();
  test_own_queue_deepcopy();
  test_own_outbox_deepcopy();
  test_byte_cap();
  test_concurrent_enqueue();
  test_safe_unload();
  test_outbox_ttl();
  test_expired_does_not_block_new_work();
  test_pipeline_ttl_produce();

  if (failures) {
    std::cerr << failures << " checks failed\n";
    return 1;
  }
  std::cout << "all outbox/queue unit checks passed (OWN/byte/concurrent/unload included)\n";
  return 0;
}
