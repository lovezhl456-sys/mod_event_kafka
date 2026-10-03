// Real core opens tool-produced databases; test fixture operations only.
// With no arguments, ctest runs the same checks against local fixtures.
#include "kafka_outbox.hpp"
#include <sqlite3.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
using namespace event_kafka;
#define CHECK(x) do { if (!(x)) throw std::runtime_error(#x + std::string(": ") + e); } while (0)

int run_mode(const std::string& mode, const std::string& path) {
  std::string e;
  Outbox b(path, mode == "migrated" ? 10 : 1, 100000);
  CHECK(b.open(e));
  if (mode == "migrated") {
    CHECK(b.requeue_in_flight(e));
    CHECK(b.fetch_due(wall_now_ms() + 1000, 10).empty());  // imported dead head remains barrier
    CHECK(b.retry_dead("b", e));
    const char* ids[] = {"b", "c", "a"};
    for (int n = 0; n < 3; ++n) {
      auto due = b.fetch_due(wall_now_ms() + 1000, 10);
      CHECK(due.size() == 1);
      CHECK(due[0].event_id == ids[n]);
      CHECK(due[0].call_seq == n);
      CHECK(b.mark_in_flight(ids[n], e));
      CHECK(b.mark_acked(ids[n], e));
    }
    OutboxRecord r;
    r.event_id = "new";
    r.topic = "t";
    r.msg_key = "k";
    r.call_uuid = "call";
    r.payload = "new";
    int64_t seq = -1;
    CHECK(b.insert_pending(r, e, &seq));
    CHECK(seq == 3);
  } else if (mode == "seed") {
    OutboxRecord r;
    r.event_id = "closed-0";
    r.topic = "t";
    r.msg_key = "k";
    r.call_uuid = "closed";
    r.payload = "p";
    CHECK(b.insert_pending(r, e));
    CHECK(b.mark_in_flight(r.event_id, e));
    CHECK(b.mark_acked(r.event_id, e));
  } else if (mode == "retired") {
    OutboxRecord r;
    r.event_id = "closed-0";
    r.topic = "t";
    r.msg_key = "k";
    r.call_uuid = "closed";
    r.payload = "p";
    CHECK(!b.insert_pending(r, e));
    CHECK(e.find("retired") != std::string::npos);
    r.event_id = "different-0";
    r.call_uuid = "different";
    CHECK(b.insert_pending(r, e));  // released capacity can be used by a different ID
  } else {
    return 2;
  }
  std::cout << mode << " PASS\n";
  return 0;
}

void exec_sql(const std::string& path, const char* text) {
  sqlite3* db = nullptr;
  if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) throw std::runtime_error("fixture open");
  char* err = nullptr;
  if (sqlite3_exec(db, text, nullptr, nullptr, &err) != SQLITE_OK) {
    std::string message = err ? err : "fixture sql";
    sqlite3_free(err);
    sqlite3_close(db);
    throw std::runtime_error(message);
  }
  sqlite3_close(db);
}

void init_schema(const std::string& path) {
  std::string e;
  Outbox b(path, 10, 100000);
  if (!b.open(e)) throw std::runtime_error(e);
  b.close();
}

int self_test() {
  const std::string root = "/tmp/event-kafka-ops-interop-" + std::to_string(getpid()) + "/";
  std::filesystem::create_directories(root);
  init_schema(root + "migrated.db");
  exec_sql(root + "migrated.db",
           "INSERT INTO call_streams(topic,call_uuid,msg_key,mode,next_seq,admit_seq) "
           "VALUES('t','call','k','admission',0,3);"
           "INSERT INTO outbox(event_id,topic,msg_key,payload,state,attempts,next_attempt_at_ms,"
           "last_error,created_at_ms,updated_at_ms,call_uuid,call_seq) VALUES"
           "('b','t','k','b','dead',1,1,'reason',1,1,'call',0),"
           "('c','t','k','c','pending',1,1,'',2,2,'call',1),"
           "('a','t','k','a','in_flight',1,1,'',3,3,'call',2);");
  if (run_mode("migrated", root + "migrated.db")) return 1;
  if (run_mode("seed", root + "seed.db")) return 1;
  init_schema(root + "retired.db");
  exec_sql(root + "retired.db",
           "INSERT INTO retired_calls(topic,call_uuid,msg_key,next_seq,retired_at_ms,reason) "
           "VALUES('t','closed','k',1,1,'explicit test closure');");
  if (run_mode("retired", root + "retired.db")) return 1;
  std::cout << "ops_interop self PASS\n";
  return 0;
}

int main(int argc, char** argv) {
  try {
    if (argc == 1) return self_test();
    if (argc != 3) return 2;
    return run_mode(argv[1], argv[2]);
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
