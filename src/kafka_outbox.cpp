#include "kafka_outbox.hpp"

#include <fcntl.h>
#include <sqlite3.h>
#include <sys/file.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <limits>
#include <mutex>
#include <random>
#include <stdexcept>

namespace event_kafka {
int64_t wall_now_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}
std::string make_event_id() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  uint64_t a = rng(), b = rng();
  char buf[37];
  std::snprintf(buf, sizeof(buf), "%08x-%04x-4%03x-%04x-%012llx", unsigned(a >> 32),
                unsigned((a >> 16) & 0xffff), unsigned(a & 0xfff),
                unsigned(0x8000 | ((b >> 48) & 0x3fff)),
                (unsigned long long)(b & 0xffffffffffffULL));
  return buf;
}
namespace {
void sql(sqlite3* db, const char* text) {
  char* error = nullptr;
  if (sqlite3_exec(db, text, nullptr, nullptr, &error) != SQLITE_OK) {
    std::string msg = error ? error : "SQLite execution failed";
    sqlite3_free(error);
    throw std::runtime_error(msg);
  }
}
struct Statement {
  sqlite3* db;
  sqlite3_stmt* s = nullptr;
  Statement(sqlite3* d, const char* q) : db(d) {
    if (!db) throw std::runtime_error("db closed");
    if (sqlite3_prepare_v2(db, q, -1, &s, nullptr) != SQLITE_OK)
      throw std::runtime_error(sqlite3_errmsg(db));
  }
  ~Statement() { sqlite3_finalize(s); }
  void bind(int i, const std::string& v) {
    if (sqlite3_bind_text(s, i, v.data(), int(v.size()), SQLITE_TRANSIENT) != SQLITE_OK)
      throw std::runtime_error("bind text");
  }
  void bind(int i, int64_t v) {
    if (sqlite3_bind_int64(s, i, v) != SQLITE_OK) throw std::runtime_error("bind integer");
  }
  bool row() {
    int rc = sqlite3_step(s);
    if (rc == SQLITE_ROW) return true;
    if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db));
    return false;
  }
  void done() {
    if (row()) throw std::runtime_error("unexpected result row");
  }
  std::string text(int i) {
    const auto* p = sqlite3_column_text(s, i);
    return p ? std::string(reinterpret_cast<const char*>(p), sqlite3_column_bytes(s, i)) : "";
  }
  int64_t integer(int i) { return sqlite3_column_int64(s, i); }
};
struct Transaction {
  sqlite3* db;
  bool committed = false;
  explicit Transaction(sqlite3* d) : db(d) {
    if (!db) throw std::runtime_error("db closed");
    sql(db, "BEGIN IMMEDIATE");
  }
  ~Transaction() {
    if (!committed) sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
  }
  void commit() {
    sql(db, "COMMIT");
    committed = true;
  }
};
const char* columns =
    "o.event_id,o.topic,o.msg_key,o.payload,o.state,o.attempts,o.next_attempt_at_ms,o.last_error,o."
    "created_at_ms,o.updated_at_ms,o.call_uuid,o.call_seq";
OutboxRecord record(Statement& s) {
  OutboxRecord r;
  r.event_id = s.text(0);
  r.topic = s.text(1);
  r.msg_key = s.text(2);
  r.payload = s.text(3);
  r.state = s.text(4) == "in_flight" ? OutboxState::InFlight
            : s.text(4) == "dead"    ? OutboxState::Dead
                                     : OutboxState::Pending;
  r.attempts = int(s.integer(5));
  r.next_attempt_at_ms = s.integer(6);
  r.last_error = s.text(7);
  r.created_at_ms = s.integer(8);
  r.updated_at_ms = s.integer(9);
  r.call_uuid = s.text(10);
  r.call_seq = sqlite3_column_type(s.s, 11) == SQLITE_NULL ? -1 : s.integer(11);
  return r;
}
}  // namespace
struct Outbox::Impl {
  std::string path;
  int64_t max_rows, max_bytes;
  sqlite3* db = nullptr;
  mutable std::mutex mu;
  int lock_fd = -1;
  Impl(std::string p, int64_t r, int64_t b) : path(std::move(p)), max_rows(r), max_bytes(b) {}
};
Outbox::Outbox(std::string p, int64_t rows, int64_t bytes)
    : impl_(new Impl{std::move(p), rows, bytes}) {}
Outbox::~Outbox() {
  close();
  delete impl_;
}
bool Outbox::open(std::string& err) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  if (impl_->db) return true;
  try {
    impl_->lock_fd = ::open((impl_->path + ".lock").c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    if (impl_->lock_fd < 0 || flock(impl_->lock_fd, LOCK_EX | LOCK_NB) != 0)
      throw std::runtime_error("outbox already owned or lock unavailable");
    if (sqlite3_open(impl_->path.c_str(), &impl_->db) != SQLITE_OK)
      throw std::runtime_error("outbox open failed");
    sqlite3_busy_timeout(impl_->db, 1000);
    // Refuse ambiguous legacy backlog. Never invent source sequence during upgrade.
    bool has_table = false, has_seq = false, has_eligible = false;
    {
      Statement q(impl_->db, "SELECT name FROM sqlite_master WHERE type='table' AND name='outbox'");
      has_table = q.row();
    }
    if (has_table) {
      Statement q(impl_->db, "PRAGMA table_info(outbox)");
      while (q.row()) {
        if (q.text(1) == "call_seq") has_seq = true;
        if (q.text(1) == "eligible_at_ms") has_eligible = true;
      }
      if (!has_seq) {
        Statement n(impl_->db, "SELECT count(*) FROM outbox");
        n.row();
        if (n.integer(0))
          throw std::runtime_error(
              "legacy nonempty outbox: reconcile/backup before upgrade; no sequence inferred");
      }
    }
    {
      Statement v(impl_->db, "PRAGMA user_version");
      v.row();
      if (v.integer(0) > 5) throw std::runtime_error("unsupported outbox schema version");
    }
    sql(impl_->db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
    Transaction tx(impl_->db);
    sql(impl_->db,
        "CREATE TABLE IF NOT EXISTS outbox(event_id TEXT PRIMARY KEY,topic TEXT NOT NULL,msg_key "
        "TEXT,payload BLOB NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT "
        "0,next_attempt_at_ms INTEGER NOT NULL,last_error TEXT,created_at_ms INTEGER NOT "
        "NULL,updated_at_ms INTEGER NOT NULL,call_uuid TEXT,call_seq INTEGER,eligible_at_ms INTEGER);");
    if (has_table && !has_seq) sql(impl_->db, "ALTER TABLE outbox ADD COLUMN call_seq INTEGER");
    if (has_table && !has_eligible)
      sql(impl_->db, "ALTER TABLE outbox ADD COLUMN eligible_at_ms INTEGER");
    sql(impl_->db,
        "CREATE TABLE IF NOT EXISTS call_streams(topic TEXT NOT NULL,call_uuid TEXT NOT "
        "NULL,msg_key TEXT NOT NULL,mode TEXT NOT NULL,next_seq INTEGER NOT NULL DEFAULT "
        "0,admit_seq INTEGER NOT NULL DEFAULT 0,last_served_ms INTEGER NOT NULL DEFAULT 0,PRIMARY "
        "KEY(topic,call_uuid));"
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_seq ON outbox(topic,call_uuid,call_seq);"
        "CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(state,next_attempt_at_ms);"
        "CREATE INDEX IF NOT EXISTS idx_outbox_ttl ON outbox(state,created_at_ms);"
        "CREATE TABLE IF NOT EXISTS retired_calls(topic TEXT NOT NULL,call_uuid TEXT NOT NULL,"
        "msg_key TEXT NOT NULL,next_seq INTEGER NOT NULL,retired_at_ms INTEGER NOT NULL,"
        "reason TEXT NOT NULL,PRIMARY KEY(topic,call_uuid));"
        "CREATE TABLE IF NOT EXISTS order_scheduler(id INTEGER PRIMARY KEY CHECK(id=1),turn "
        "INTEGER NOT NULL);"
        "INSERT OR IGNORE INTO order_scheduler SELECT 1,COALESCE(MAX(last_served_ms),0) FROM "
        "call_streams;");
    // New-schema rows without their durable cursor cannot be safely scheduled.
    {
      Statement q(impl_->db,
                  "SELECT count(*) FROM outbox o LEFT JOIN call_streams s ON o.topic=s.topic AND "
                  "o.call_uuid=s.call_uuid WHERE COALESCE(o.call_uuid,'')<>'' AND (o.call_seq IS "
                  "NULL OR s.call_uuid IS NULL)");
      q.row();
      if (q.integer(0)) throw std::runtime_error("corrupt ordered outbox/cursor");
    }
    {
      Statement q(impl_->db, "SELECT count(*) FROM retired_calls r JOIN call_streams s ON "
                            "r.topic=s.topic AND r.call_uuid=s.call_uuid");
      q.row();
      if (q.integer(0)) throw std::runtime_error("retired call still has active cursor");
    }
    // Stamp deliverable age only for rows that are already the head or ungrouped.
    // Waiting successors stay NULL until the cursor reaches them. Sequences are untouched.
    sql(impl_->db,
        "UPDATE outbox SET eligible_at_ms=created_at_ms WHERE eligible_at_ms IS NULL AND ("
        "COALESCE(call_uuid,'')='' OR call_seq=(SELECT next_seq FROM call_streams s WHERE "
        "s.topic=outbox.topic AND s.call_uuid=outbox.call_uuid)); PRAGMA user_version=5;");
    tx.commit();
    return true;
  } catch (const std::exception& e) {
    err = e.what();
    if (impl_->db) sqlite3_close(impl_->db);
    impl_->db = nullptr;
    if (impl_->lock_fd >= 0) ::close(impl_->lock_fd);
    impl_->lock_fd = -1;
    return false;
  }
}
void Outbox::close() {
  std::lock_guard<std::mutex> lock(impl_->mu);
  if (impl_->db) sqlite3_close(impl_->db);
  impl_->db = nullptr;
  if (impl_->lock_fd >= 0) ::close(impl_->lock_fd);
  impl_->lock_fd = -1;
}
bool Outbox::insert_pending(const OutboxRecord& rec, std::string& err, int64_t* assigned_sequence) {
  if (assigned_sequence) *assigned_sequence = -1;
  std::lock_guard<std::mutex> lock(impl_->mu);
  try {
    Transaction tx(impl_->db);
    if (!rec.call_uuid.empty()) {
      {
        Statement retired(impl_->db, "SELECT 1 FROM retired_calls WHERE topic=? AND call_uuid=?");
        retired.bind(1, rec.topic);
        retired.bind(2, rec.call_uuid);
        if (retired.row()) throw std::runtime_error("call explicitly retired; reuse/late admission rejected");
      }
    }
    // Only ungrouped TTL rows may be reclaimed. Ordered dead rows are durable barriers.
    {
      Statement q(impl_->db, "SELECT count(*),COALESCE(sum(length(payload)),0) FROM outbox");
      q.row();
      if (q.integer(0) >= impl_->max_rows ||
          q.integer(1) + int64_t(rec.payload.size()) > impl_->max_bytes)
        sql(impl_->db,
            "DELETE FROM outbox WHERE state='dead' AND last_error='expired_ttl' AND "
            "COALESCE(call_uuid,'')=''");
    }
    {
      Statement q(impl_->db, "SELECT count(*),COALESCE(sum(length(payload)),0) FROM outbox");
      q.row();
      if (q.integer(0) >= impl_->max_rows ||
          q.integer(1) + int64_t(rec.payload.size()) > impl_->max_bytes)
        throw std::runtime_error("outbox capacity exceeded");
    }
    int64_t seq = rec.call_seq;
    bool deliverable_now = rec.call_uuid.empty();
    if (!rec.call_uuid.empty()) {
      if (rec.msg_key.empty()) throw std::runtime_error("ordered call requires nonempty key");
      const std::string mode = seq < 0 ? "admission" : "source";
      {
        Statement q(impl_->db,
                    "SELECT msg_key,mode,next_seq,admit_seq FROM call_streams WHERE topic=? AND "
                    "call_uuid=?");
        q.bind(1, rec.topic);
        q.bind(2, rec.call_uuid);
        if (q.row()) {
          if (q.text(0) != rec.msg_key || q.text(1) != mode)
            throw std::runtime_error("call key/order mode changed");
          const int64_t cursor = q.integer(2);
          if (seq < 0)
            seq = q.integer(3);
          else if (seq < cursor)
            throw std::runtime_error("source sequence already acknowledged");
          deliverable_now = seq == cursor;
        } else {
          Statement count(impl_->db, "SELECT count(*) FROM call_streams");
          count.row();
          if (count.integer(0) >= impl_->max_rows)
            throw std::runtime_error(
                "stream capacity exceeded; inspect capacity; raise limit or explicitly retire proven closed calls");
          Statement create(
              impl_->db, "INSERT INTO call_streams(topic,call_uuid,msg_key,mode) VALUES(?,?,?,?)");
          create.bind(1, rec.topic);
          create.bind(2, rec.call_uuid);
          create.bind(3, rec.msg_key);
          create.bind(4, mode);
          create.done();
          if (seq < 0) seq = 0;
          deliverable_now = seq == 0;  // a new stream's cursor starts at 0
        }
      }
      if (seq < 0 || seq == std::numeric_limits<int64_t>::max())
        throw std::runtime_error("source sequence out of range");
      Statement update(
          impl_->db,
          "UPDATE call_streams SET admit_seq=MAX(admit_seq,?) WHERE topic=? AND call_uuid=?");
      update.bind(1, seq + 1);
      update.bind(2, rec.topic);
      update.bind(3, rec.call_uuid);
      update.done();
    } else if (seq >= 0)
      throw std::runtime_error("source sequence requires call_uuid");
    Statement q(
        impl_->db,
        "INSERT INTO "
        "outbox(event_id,topic,msg_key,payload,state,attempts,next_attempt_at_ms,last_error,"
        "created_at_ms,updated_at_ms,call_uuid,call_seq,eligible_at_ms) "
        "VALUES(?,?,?,?,'pending',0,?,'',?,?,?,?,?)");
    int64_t now = rec.created_at_ms ? rec.created_at_ms : wall_now_ms();
    q.bind(1, rec.event_id);
    q.bind(2, rec.topic);
    q.bind(3, rec.msg_key);
    if (sqlite3_bind_blob(q.s, 4, rec.payload.data(), int(rec.payload.size()), SQLITE_TRANSIENT) !=
        SQLITE_OK)
      throw std::runtime_error("bind payload");
    q.bind(5, rec.next_attempt_at_ms ? rec.next_attempt_at_ms : now);
    q.bind(6, now);
    q.bind(7, now);
    q.bind(8, rec.call_uuid);
    if (rec.call_uuid.empty())
      sqlite3_bind_null(q.s, 9);
    else
      q.bind(9, seq);
    if (deliverable_now)
      q.bind(10, now);
    else
      sqlite3_bind_null(q.s, 10);
    q.done();
    tx.commit();
    if (assigned_sequence) *assigned_sequence = rec.call_uuid.empty() ? -1 : seq;
    return true;
  } catch (const std::exception& e) {
    err = e.what();
    return false;
  }
}
std::vector<OutboxRecord> Outbox::fetch_due(int64_t now, int limit) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  std::vector<OutboxRecord> result;
  if (!impl_->db) return result;
  // Cursor equality includes missing heads, retries, in-flight, and dead heads.
  std::string query = std::string("SELECT ") + columns +
                      " FROM outbox o LEFT JOIN call_streams s ON o.topic=s.topic AND "
                      "o.call_uuid=s.call_uuid WHERE o.state='pending' AND o.next_attempt_at_ms<=? "
                      "AND (COALESCE(o.call_uuid,'')='' OR o.call_seq=s.next_seq) ORDER BY "
                      "COALESCE(s.last_served_ms,0),o.created_at_ms,o.rowid LIMIT ?";
  Statement q(impl_->db, query.c_str());
  q.bind(1, now);
  q.bind(2, int64_t(limit));
  while (q.row()) result.push_back(record(q));
  return result;
}
bool Outbox::mark_in_flight(const std::string& id, std::string& err) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  try {
    Transaction tx(impl_->db);
    Statement q(
        impl_->db,
        "UPDATE outbox SET state='in_flight',attempts=attempts+1,updated_at_ms=? WHERE event_id=? "
        "AND state='pending' AND (COALESCE(call_uuid,'')='' OR call_seq=(SELECT next_seq FROM "
        "call_streams s WHERE s.topic=outbox.topic AND s.call_uuid=outbox.call_uuid))");
    q.bind(1, wall_now_ms());
    q.bind(2, id);
    q.done();
    if (sqlite3_changes(impl_->db) != 1) throw std::runtime_error("not a pending call head");
    // Historical column name retained on migration; its value is now a durable service turn,
    // not wall time. Reboots and wall-clock rollback cannot repeatedly favor the same calls.
    Statement tick(
        impl_->db,
        "UPDATE order_scheduler SET turn=turn+1 WHERE id=1 AND turn<9223372036854775807");
    tick.done();
    if (sqlite3_changes(impl_->db) != 1) throw std::runtime_error("scheduler turn exhausted");
    Statement served(impl_->db,
                     "UPDATE call_streams SET last_served_ms=(SELECT turn FROM order_scheduler "
                     "WHERE id=1) WHERE (topic,call_uuid)=(SELECT "
                     "topic,call_uuid FROM outbox WHERE event_id=?)");
    served.bind(1, id);
    served.done();
    tx.commit();
    return true;
  } catch (const std::exception& e) {
    err = e.what();
    return false;
  }
}
bool Outbox::mark_acked(const std::string& id, std::string& err) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  try {
    Transaction tx(impl_->db);
    Statement head(
        impl_->db,
        "SELECT topic,call_uuid,call_seq FROM outbox WHERE event_id=? AND state='in_flight'");
    head.bind(1, id);
    if (!head.row()) throw std::runtime_error("ACK for non-in-flight event");
    const std::string topic = head.text(0);
    const std::string call = head.text(1);
    const int64_t seq = head.integer(2);
    if (!call.empty()) {
      Statement cursor(impl_->db,
                       "UPDATE call_streams SET next_seq=next_seq+1 WHERE topic=? AND call_uuid=? "
                       "AND next_seq=?");
      cursor.bind(1, topic);
      cursor.bind(2, call);
      cursor.bind(3, seq);
      cursor.done();
      if (sqlite3_changes(impl_->db) != 1) throw std::runtime_error("ACK is not the call head");
      // The successor's deliverable window starts now, not at its admission time.
      Statement promote(
          impl_->db,
          "UPDATE outbox SET eligible_at_ms=? WHERE topic=? AND call_uuid=? AND call_seq=? AND "
          "eligible_at_ms IS NULL");
      const int64_t became_head = wall_now_ms();
      promote.bind(1, became_head);
      promote.bind(2, topic);
      promote.bind(3, call);
      promote.bind(4, seq + 1);
      promote.done();
    }
    Statement del(impl_->db, "DELETE FROM outbox WHERE event_id=?");
    del.bind(1, id);
    del.done();
    tx.commit();
    return true;
  } catch (const std::exception& e) {
    err = e.what();
    return false;
  }
}
bool Outbox::mark_retry(const std::string& id, int64_t next, const std::string& error,
                        std::string& err) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  try {
    Statement q(
        impl_->db,
        "UPDATE outbox SET state='pending',next_attempt_at_ms=?,last_error=?,updated_at_ms=? WHERE "
        "event_id=? AND state='in_flight'");
    q.bind(1, next);
    q.bind(2, error);
    q.bind(3, wall_now_ms());
    q.bind(4, id);
    q.done();
    if (sqlite3_changes(impl_->db) != 1) throw std::runtime_error("retry for non-in-flight event");
    return true;
  } catch (const std::exception& e) {
    err = e.what();
    return false;
  }
}
bool Outbox::mark_dead(const std::string& id, const std::string& error, std::string& err) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  try {
    Statement q(impl_->db,
                "UPDATE outbox SET state='dead',last_error=?,updated_at_ms=? WHERE event_id=? AND "
                "state IN ('pending','in_flight')");
    q.bind(1, error);
    q.bind(2, wall_now_ms());
    q.bind(3, id);
    q.done();
    if (sqlite3_changes(impl_->db) != 1)
      throw std::runtime_error("dead transition has no live row");
    return true;
  } catch (const std::exception& e) {
    err = e.what();
    return false;
  }
}
bool Outbox::retry_dead(const std::string& id, std::string& err) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  // Explicit operator retry of the SAME event; never deletes/skips or renumbers it.
  try {
    Statement q(
        impl_->db,
        "UPDATE outbox SET state='pending',next_attempt_at_ms=?,updated_at_ms=?,last_error='' "
        "WHERE event_id=? AND state='dead'");
    q.bind(1, wall_now_ms());
    q.bind(2, wall_now_ms());
    q.bind(3, id);
    q.done();
    if (sqlite3_changes(impl_->db) != 1) throw std::runtime_error("no dead row to retry");
    return true;
  } catch (const std::exception& e) {
    err = e.what();
    return false;
  }
}
bool Outbox::requeue_in_flight(std::string& err) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  try {
    Statement q(impl_->db,
                "UPDATE outbox SET state='pending',next_attempt_at_ms=?,updated_at_ms=? WHERE "
                "state='in_flight'");
    q.bind(1, wall_now_ms());
    q.bind(2, wall_now_ms());
    q.done();
    return true;
  } catch (const std::exception& e) {
    err = e.what();
    return false;
  }
}
int Outbox::expire_ttl(int64_t now, int64_t ttl, std::string& err) {
  if (ttl <= 0) return 0;
  std::lock_guard<std::mutex> lock(impl_->mu);
  try {
    Statement q(
        impl_->db,
        "UPDATE outbox SET state='dead',last_error='expired_ttl',updated_at_ms=? WHERE "
        "state='pending' AND eligible_at_ms IS NOT NULL AND (? - eligible_at_ms)>? AND ("
        "COALESCE(call_uuid,'')='' OR call_seq=(SELECT next_seq FROM call_streams s WHERE "
        "s.topic=outbox.topic AND s.call_uuid=outbox.call_uuid))");
    q.bind(1, now);
    q.bind(2, now);
    q.bind(3, ttl);
    q.done();
    return sqlite3_changes(impl_->db);
  } catch (const std::exception& e) {
    err = e.what();
    return -1;
  }
}
bool Outbox::get(const std::string& id, OutboxRecord& out) const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  if (!impl_->db) return false;
  std::string query = std::string("SELECT ") + columns + " FROM outbox o WHERE event_id=?";
  Statement q(impl_->db, query.c_str());
  q.bind(1, id);
  if (!q.row()) return false;
  out = record(q);
  return true;
}
OutboxStats Outbox::stats() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  OutboxStats st;
  if (!impl_->db) return st;
  Statement q(impl_->db,
              "SELECT state,count(*),COALESCE(sum(length(payload)),0) FROM outbox GROUP BY state");
  while (q.row()) {
    if (q.text(0) == "pending")
      st.pending = q.integer(1);
    else if (q.text(0) == "in_flight")
      st.in_flight = q.integer(1);
    else if (q.text(0) == "dead")
      st.dead = q.integer(1);
    st.total_bytes += q.integer(2);
  }
  return st;
}
int64_t Outbox::oldest_pending_age_ms(int64_t now) const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  if (!impl_->db) return 0;
  Statement q(impl_->db, "SELECT min(created_at_ms) FROM outbox WHERE state='pending'");
  q.row();
  if (sqlite3_column_type(q.s, 0) == SQLITE_NULL) return 0;
  return std::max<int64_t>(0, now - q.integer(0));
}
}  // namespace event_kafka
