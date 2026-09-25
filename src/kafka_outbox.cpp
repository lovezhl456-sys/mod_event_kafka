#include "kafka_outbox.hpp"

#include <sqlite3.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <random>
#include <string>

namespace event_kafka {

int64_t wall_now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string make_event_id() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<uint64_t> dist;
  const uint64_t a = dist(rng);
  const uint64_t b = dist(rng);
  char buf[37];
  std::snprintf(buf, sizeof(buf), "%08x-%04x-4%03x-%04x-%012llx",
                (unsigned)((a >> 32) & 0xffffffffu),
                (unsigned)((a >> 16) & 0xffffu),
                (unsigned)(a & 0x0fffu),
                (unsigned)(0x8000u | ((b >> 48) & 0x3fffu)),
                (unsigned long long)(b & 0xffffffffffffULL));
  return std::string(buf);
}

namespace {

OutboxRecord read_outbox_row(sqlite3_stmt* stmt) {
  OutboxRecord r;
  r.event_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
  r.topic = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
  r.msg_key = sqlite3_column_text(stmt, 2)
                  ? reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2))
                  : "";
  const void* blob = sqlite3_column_blob(stmt, 3);
  const int blen = sqlite3_column_bytes(stmt, 3);
  r.payload.assign(static_cast<const char*>(blob), static_cast<const char*>(blob) + blen);
  const char* state = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
  r.state = OutboxState::Pending;
  if (state && std::strcmp(state, "in_flight") == 0) r.state = OutboxState::InFlight;
  else if (state && std::strcmp(state, "dead") == 0) r.state = OutboxState::Dead;
  r.attempts = sqlite3_column_int(stmt, 5);
  r.next_attempt_at_ms = sqlite3_column_int64(stmt, 6);
  r.last_error = sqlite3_column_text(stmt, 7)
                     ? reinterpret_cast<const char*>(sqlite3_column_text(stmt, 7))
                     : "";
  r.created_at_ms = sqlite3_column_int64(stmt, 8);
  r.updated_at_ms = sqlite3_column_int64(stmt, 9);
  r.call_uuid = sqlite3_column_text(stmt, 10)
                    ? reinterpret_cast<const char*>(sqlite3_column_text(stmt, 10))
                    : "";
  return r;
}

bool query_counts(sqlite3* db, const char* where_sql, int64_t& rows, int64_t& bytes) {
  std::string sql = "SELECT COUNT(*), IFNULL(SUM(LENGTH(payload)),0) FROM outbox";
  if (where_sql && where_sql[0]) {
    sql.push_back(' ');
    sql += where_sql;
  }
  sqlite3_stmt* cs = nullptr;
  if (sqlite3_prepare_v2(db, sql.c_str(), -1, &cs, nullptr) != SQLITE_OK) return false;
  const bool ok = sqlite3_step(cs) == SQLITE_ROW;
  if (ok) {
    rows = sqlite3_column_int64(cs, 0);
    bytes = sqlite3_column_int64(cs, 1);
  }
  sqlite3_finalize(cs);
  return ok;
}

// Expired dead-letter rows must not block a new insert. Pending, in-flight, and
// other dead rows (permanent errors) stay. Returns false when the new payload
// still does not fit among those rows.
bool admit_insert(sqlite3* db, int64_t max_rows, int64_t max_bytes, int64_t extra_bytes,
                  std::string& err) {
  int64_t rows = 0;
  int64_t bytes = 0;
  if (!query_counts(db, nullptr, rows, bytes)) {
    err = "outbox capacity check failed";
    return false;
  }
  if (rows < max_rows && bytes + extra_bytes <= max_bytes) return true;

  int64_t blocking_rows = 0;
  int64_t blocking_bytes = 0;
  if (!query_counts(db, "WHERE NOT (state='dead' AND last_error='expired_ttl')", blocking_rows,
                    blocking_bytes)) {
    err = "outbox capacity check failed";
    return false;
  }
  if (blocking_rows >= max_rows || blocking_bytes + extra_bytes > max_bytes) {
    err = "outbox capacity exceeded";
    return false;
  }

  char* errmsg = nullptr;
  if (sqlite3_exec(db, "DELETE FROM outbox WHERE state='dead' AND last_error='expired_ttl'", nullptr,
                   nullptr, &errmsg) != SQLITE_OK) {
    err = errmsg ? errmsg : "expired row reclaim failed";
    sqlite3_free(errmsg);
    return false;
  }
  return true;
}

}  // namespace

struct Outbox::Impl {
  std::string path;
  int64_t max_rows;
  int64_t max_bytes;
  sqlite3* db{nullptr};
  mutable std::mutex mu;
};

Outbox::Outbox(std::string db_path, int64_t max_rows, int64_t max_bytes)
    : impl_(new Impl{std::move(db_path), max_rows, max_bytes, nullptr}) {}

Outbox::~Outbox() {
  close();
  delete impl_;
  impl_ = nullptr;
}

bool Outbox::open(std::string& err) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (impl_->db) return true;
  if (sqlite3_open(impl_->path.c_str(), &impl_->db) != SQLITE_OK) {
    err = impl_->db ? sqlite3_errmsg(impl_->db) : "sqlite3_open failed";
    if (impl_->db) {
      sqlite3_close(impl_->db);
      impl_->db = nullptr;
    }
    return false;
  }
  char* errmsg = nullptr;
  const char* ddl =
      "PRAGMA journal_mode=WAL;"
      "PRAGMA synchronous=NORMAL;"
      "CREATE TABLE IF NOT EXISTS outbox ("
      "  event_id TEXT PRIMARY KEY,"
      "  topic TEXT NOT NULL,"
      "  msg_key TEXT,"
      "  payload BLOB NOT NULL,"
      "  state TEXT NOT NULL,"
      "  attempts INTEGER NOT NULL DEFAULT 0,"
      "  next_attempt_at_ms INTEGER NOT NULL,"
      "  last_error TEXT,"
      "  created_at_ms INTEGER NOT NULL,"
      "  updated_at_ms INTEGER NOT NULL,"
      "  call_uuid TEXT"
      ");"
      "CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(state, next_attempt_at_ms);"
      "CREATE INDEX IF NOT EXISTS idx_outbox_call ON outbox(call_uuid, created_at_ms);"
      "CREATE INDEX IF NOT EXISTS idx_outbox_ttl ON outbox(state, created_at_ms);";
  if (sqlite3_exec(impl_->db, ddl, nullptr, nullptr, &errmsg) != SQLITE_OK) {
    err = errmsg ? errmsg : "ddl failed";
    sqlite3_free(errmsg);
    sqlite3_close(impl_->db);
    impl_->db = nullptr;
    return false;
  }
  return true;
}

void Outbox::close() {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (impl_->db) {
    sqlite3_close(impl_->db);
    impl_->db = nullptr;
  }
}

bool Outbox::insert_pending(const OutboxRecord& rec, std::string& err) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) {
    err = "db closed";
    return false;
  }

  if (!admit_insert(impl_->db, impl_->max_rows, impl_->max_bytes,
                    static_cast<int64_t>(rec.payload.size()), err)) {
    return false;
  }

  sqlite3_stmt* stmt = nullptr;
  const char* sql =
      "INSERT INTO outbox(event_id,topic,msg_key,payload,state,attempts,next_attempt_at_ms,"
      "last_error,created_at_ms,updated_at_ms,call_uuid) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
  if (sqlite3_prepare_v2(impl_->db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    err = sqlite3_errmsg(impl_->db);
    return false;
  }
  const int64_t ts = rec.created_at_ms ? rec.created_at_ms : wall_now_ms();
  sqlite3_bind_text(stmt, 1, rec.event_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, rec.topic.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 3, rec.msg_key.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_blob(stmt, 4, rec.payload.data(), (int)rec.payload.size(), SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 5, "pending", -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 6, 0);
  sqlite3_bind_int64(stmt, 7, rec.next_attempt_at_ms ? rec.next_attempt_at_ms : ts);
  sqlite3_bind_text(stmt, 8, "", -1, SQLITE_STATIC);
  sqlite3_bind_int64(stmt, 9, ts);
  sqlite3_bind_int64(stmt, 10, ts);
  sqlite3_bind_text(stmt, 11, rec.call_uuid.c_str(), -1, SQLITE_TRANSIENT);
  const bool ok = sqlite3_step(stmt) == SQLITE_DONE;
  if (!ok) err = sqlite3_errmsg(impl_->db);
  sqlite3_finalize(stmt);
  return ok;
}

std::vector<OutboxRecord> Outbox::fetch_due(int64_t now, int limit) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  std::vector<OutboxRecord> out;
  if (!impl_->db) return out;
  sqlite3_stmt* stmt = nullptr;
  const char* sql =
      "SELECT event_id,topic,msg_key,payload,state,attempts,next_attempt_at_ms,last_error,"
      "created_at_ms,updated_at_ms,call_uuid FROM outbox "
      "WHERE state='pending' AND next_attempt_at_ms<=? "
      "ORDER BY CASE WHEN call_uuid IS NULL OR call_uuid='' THEN 1 ELSE 0 END, "
      "call_uuid, created_at_ms LIMIT ?";
  if (sqlite3_prepare_v2(impl_->db, sql, -1, &stmt, nullptr) != SQLITE_OK) return out;
  sqlite3_bind_int64(stmt, 1, now);
  sqlite3_bind_int(stmt, 2, limit);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    out.push_back(read_outbox_row(stmt));
  }
  sqlite3_finalize(stmt);
  return out;
}

bool Outbox::mark_in_flight(const std::string& event_id, std::string& err) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) {
    err = "db closed";
    return false;
  }
  sqlite3_stmt* stmt = nullptr;
  const char* sql =
      "UPDATE outbox SET state='in_flight', attempts=attempts+1, updated_at_ms=? "
      "WHERE event_id=? AND state='pending'";
  if (sqlite3_prepare_v2(impl_->db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    err = sqlite3_errmsg(impl_->db);
    return false;
  }
  sqlite3_bind_int64(stmt, 1, wall_now_ms());
  sqlite3_bind_text(stmt, 2, event_id.c_str(), -1, SQLITE_TRANSIENT);
  const bool ok = sqlite3_step(stmt) == SQLITE_DONE && sqlite3_changes(impl_->db) == 1;
  if (!ok) err = "mark_in_flight failed";
  sqlite3_finalize(stmt);
  return ok;
}

bool Outbox::mark_acked(const std::string& event_id, std::string& err) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) {
    err = "db closed";
    return false;
  }
  sqlite3_stmt* stmt = nullptr;
  if (sqlite3_prepare_v2(impl_->db, "DELETE FROM outbox WHERE event_id=?", -1, &stmt, nullptr) !=
      SQLITE_OK) {
    err = sqlite3_errmsg(impl_->db);
    return false;
  }
  sqlite3_bind_text(stmt, 1, event_id.c_str(), -1, SQLITE_TRANSIENT);
  const bool ok = sqlite3_step(stmt) == SQLITE_DONE;
  if (!ok) err = sqlite3_errmsg(impl_->db);
  sqlite3_finalize(stmt);
  return ok;
}

bool Outbox::mark_retry(const std::string& event_id, int64_t next_ms, const std::string& error,
                        std::string& err) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) {
    err = "db closed";
    return false;
  }
  sqlite3_stmt* stmt = nullptr;
  const char* sql =
      "UPDATE outbox SET state='pending', next_attempt_at_ms=?, last_error=?, updated_at_ms=? "
      "WHERE event_id=?";
  if (sqlite3_prepare_v2(impl_->db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    err = sqlite3_errmsg(impl_->db);
    return false;
  }
  sqlite3_bind_int64(stmt, 1, next_ms);
  sqlite3_bind_text(stmt, 2, error.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt, 3, wall_now_ms());
  sqlite3_bind_text(stmt, 4, event_id.c_str(), -1, SQLITE_TRANSIENT);
  const bool ok = sqlite3_step(stmt) == SQLITE_DONE;
  if (!ok) err = sqlite3_errmsg(impl_->db);
  sqlite3_finalize(stmt);
  return ok;
}

bool Outbox::mark_dead(const std::string& event_id, const std::string& error, std::string& err) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) {
    err = "db closed";
    return false;
  }
  sqlite3_stmt* stmt = nullptr;
  const char* sql = "UPDATE outbox SET state='dead', last_error=?, updated_at_ms=? WHERE event_id=?";
  if (sqlite3_prepare_v2(impl_->db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    err = sqlite3_errmsg(impl_->db);
    return false;
  }
  sqlite3_bind_text(stmt, 1, error.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt, 2, wall_now_ms());
  sqlite3_bind_text(stmt, 3, event_id.c_str(), -1, SQLITE_TRANSIENT);
  const bool ok = sqlite3_step(stmt) == SQLITE_DONE;
  if (!ok) err = sqlite3_errmsg(impl_->db);
  sqlite3_finalize(stmt);
  return ok;
}

bool Outbox::requeue_in_flight(std::string& err) {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) {
    err = "db closed";
    return false;
  }
  char* errmsg = nullptr;
  const std::string sql =
      "UPDATE outbox SET state='pending', next_attempt_at_ms=" + std::to_string(wall_now_ms()) +
      ", updated_at_ms=" + std::to_string(wall_now_ms()) + " WHERE state='in_flight'";
  if (sqlite3_exec(impl_->db, sql.c_str(), nullptr, nullptr, &errmsg) != SQLITE_OK) {
    err = errmsg ? errmsg : "requeue failed";
    sqlite3_free(errmsg);
    return false;
  }
  return true;
}

int Outbox::expire_ttl(int64_t now_ms, int64_t ttl_ms, std::string& err) {
  if (ttl_ms <= 0) return 0;
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) {
    err = "db closed";
    return -1;
  }
  sqlite3_stmt* stmt = nullptr;
  const char* sql =
      "UPDATE outbox SET state='dead', last_error='expired_ttl', updated_at_ms=? "
      "WHERE state='pending' AND (? - created_at_ms) > ?";
  if (sqlite3_prepare_v2(impl_->db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    err = sqlite3_errmsg(impl_->db);
    return -1;
  }
  sqlite3_bind_int64(stmt, 1, now_ms);
  sqlite3_bind_int64(stmt, 2, now_ms);
  sqlite3_bind_int64(stmt, 3, ttl_ms);
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    err = sqlite3_errmsg(impl_->db);
    sqlite3_finalize(stmt);
    return -1;
  }
  const int changed = sqlite3_changes(impl_->db);
  sqlite3_finalize(stmt);
  return changed;
}

bool Outbox::get(const std::string& event_id, OutboxRecord& out) const {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) return false;
  sqlite3_stmt* stmt = nullptr;
  const char* sql =
      "SELECT event_id,topic,msg_key,payload,state,attempts,next_attempt_at_ms,last_error,"
      "created_at_ms,updated_at_ms,call_uuid FROM outbox WHERE event_id=?";
  if (sqlite3_prepare_v2(impl_->db, sql, -1, &stmt, nullptr) != SQLITE_OK) return false;
  sqlite3_bind_text(stmt, 1, event_id.c_str(), -1, SQLITE_TRANSIENT);
  const bool found = sqlite3_step(stmt) == SQLITE_ROW;
  if (found) out = read_outbox_row(stmt);
  sqlite3_finalize(stmt);
  return found;
}

OutboxStats Outbox::stats() const {
  std::lock_guard<std::mutex> lk(impl_->mu);
  OutboxStats st;
  if (!impl_->db) return st;
  sqlite3_stmt* stmt = nullptr;
  sqlite3_prepare_v2(impl_->db,
                     "SELECT state, COUNT(*), IFNULL(SUM(LENGTH(payload)),0) FROM outbox GROUP BY state",
                     -1, &stmt, nullptr);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const char* s = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    const int64_t c = sqlite3_column_int64(stmt, 1);
    const int64_t b = sqlite3_column_int64(stmt, 2);
    st.total_bytes += b;
    if (std::string(s) == "pending") st.pending = c;
    else if (std::string(s) == "in_flight") st.in_flight = c;
    else if (std::string(s) == "dead") st.dead = c;
  }
  sqlite3_finalize(stmt);
  return st;
}

int64_t Outbox::oldest_pending_age_ms(int64_t now) const {
  std::lock_guard<std::mutex> lk(impl_->mu);
  if (!impl_->db) return 0;
  sqlite3_stmt* stmt = nullptr;
  sqlite3_prepare_v2(impl_->db, "SELECT MIN(created_at_ms) FROM outbox WHERE state='pending'", -1,
                     &stmt, nullptr);
  int64_t age = 0;
  if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
    age = now - sqlite3_column_int64(stmt, 0);
  }
  sqlite3_finalize(stmt);
  return age < 0 ? 0 : age;
}

}  // namespace event_kafka
