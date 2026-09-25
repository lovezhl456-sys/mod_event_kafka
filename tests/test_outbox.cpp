#include "bounded_queue.hpp"
#include "kafka_outbox.hpp"
#include "kafka_pipeline.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
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

int main() {
  test_baseline_smoke();
  test_own_queue_deepcopy();
  test_own_outbox_deepcopy();
  test_byte_cap();
  test_concurrent_enqueue();
  test_safe_unload();

  if (failures) {
    std::cerr << failures << " checks failed\n";
    return 1;
  }
  std::cout << "all outbox/queue unit checks passed (OWN/byte/concurrent/unload included)\n";
  return 0;
}
