// Real-broker driver. Explicit source sequence is generated before concurrent admission.
#include <sqlite3.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "kafka_pipeline.hpp"
using namespace event_kafka;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
void snapshot(const fs::path& root) {
  sqlite3* db = nullptr;
  sqlite3_open_v2((root / "outbox.db").c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
  std::ofstream out(root / "final.tsv");
  sqlite3_stmt* q = nullptr;
  if (sqlite3_prepare_v2(
          db,
          "SELECT event_id,call_uuid,call_seq,state,attempts,last_error,created_at_ms FROM outbox "
          "ORDER BY call_uuid,call_seq",
          -1, &q, nullptr) == SQLITE_OK) {
    while (sqlite3_step(q) == SQLITE_ROW) {
      for (int i = 0; i < 7; i++) {
        if (i) out << '\t';
        const auto* t = sqlite3_column_text(q, i);
        if (t) out << t;
      }
      out << '\n';
    }
  }
  sqlite3_finalize(q);
  std::ofstream cursors(root / "cursors.tsv");
  if (sqlite3_prepare_v2(
          db, "SELECT topic,call_uuid,mode,next_seq,admit_seq FROM call_streams ORDER BY call_uuid",
          -1, &q, nullptr) == SQLITE_OK) {
    while (sqlite3_step(q) == SQLITE_ROW) {
      for (int i = 0; i < 5; i++) {
        if (i) cursors << '\t';
        cursors << sqlite3_column_text(q, i);
      }
      cursors << '\n';
    }
  }
  sqlite3_finalize(q);
  sqlite3_close(db);
}
int main(int argc, char** argv) {
  try {
    if (argc != 7) throw std::runtime_error("RUN TOPIC MODE CALLS EVENTS INTERVAL_MS");
    fs::path root = argv[1];
    std::string topic = argv[2], mode = argv[3];
    int calls = std::stoi(argv[4]), events = std::stoi(argv[5]), interval = std::stoi(argv[6]);
    bool resume = mode == "resume";
    PipelineConfig cfg;
    cfg.brokers = "127.0.0.1:39092";
    cfg.topic = topic;
    cfg.outbox_path = (root / "outbox.db").string();
    cfg.compression = "none";
    cfg.message_timeout_ms = 1000;
    cfg.outbox_ttl_ms = mode == "ttl" ? 1500 : 0;
    cfg.worker_idle_ms = 5;
    cfg.poll_ms = 5;
    cfg.require_source_sequence = true;
    KafkaPipeline p(cfg);
    std::string err;
    if (!p.start(err)) throw std::runtime_error(err);
    std::ofstream status(root / (resume ? "status-resume.jsonl" : "status.jsonl"));
    std::ofstream admissions(root / "admissions.tsv", std::ios::app);
    std::mutex logmu;
    if (!resume) {
      std::ofstream plan(root / "source-plan.tsv");
      for (int seq = 0; seq < events; seq++)
        for (int call = 0; call < calls; call++)
          plan << "call-" << call << '\t' << seq << '\t' << wall_now_ms() << '\n';
    }
    std::atomic<bool> done{resume};
    std::atomic<int> rejected{0};
    auto send = [&](int call, int seq) {
      std::string key = "call-" + std::to_string(call), id, e;
      std::string payload = "{\"call_id\":\"" + key + "\",\"sequence\":" + std::to_string(seq);
      if (mode == "poison" && call == 0 && seq == 0)
        payload += ",\"padding\":\"" + std::string(1500000, 'x') + "\"";
      payload += "}";
      auto begin = Clock::now();
      bool ok = p.enqueue(payload, key, key, id, e, seq);
      auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - begin).count();
      {
        std::lock_guard<std::mutex> lock(logmu);
        admissions << id << '\t' << key << '\t' << seq << '\t' << wall_now_ms() << '\t' << us
                   << '\t' << ok << '\t' << e << '\n';
        admissions.flush();
      }
      if (!ok) ++rejected;
    };
    std::thread generator;
    if (!resume)
      generator = std::thread([&] {
        if (mode == "concurrent") {
          std::vector<std::thread> senders;
          for (int lane = 0; lane < 8; lane++)
            senders.emplace_back([&, lane] {
              if (lane == 0) std::this_thread::sleep_for(std::chrono::milliseconds(80));
              for (int seq = lane; seq < events; seq += 8)
                for (int call = 0; call < calls; call++) send(call, seq);
            });
          for (auto& t : senders) t.join();
        } else {
          for (int seq = 0; seq < events; seq++) {
            for (int call = 0; call < calls; call++) send(call, seq);
            if (interval) std::this_thread::sleep_for(std::chrono::milliseconds(interval));
          }
        }
        done = true;
        std::ofstream(root / "admission.done") << "durable admissions complete\n";
      });
    auto began = Clock::now();
    bool drained = false;
    int64_t peak_rows = 0, peak_bytes = 0;
    while (Clock::now() - began < std::chrono::seconds(80)) {
      if (fs::exists(root / "rebuild")) {
        p.request_rebuild();
        fs::remove(root / "rebuild");
      }
      auto st = p.outbox_stats();
      auto& m = p.metrics();
      peak_rows = std::max(peak_rows, st.pending + st.in_flight + st.dead);
      peak_bytes = std::max(peak_bytes, st.total_bytes);
      status << "{\"time_ms\":" << wall_now_ms() << ",\"pid\":" << getpid()
             << ",\"acked\":" << m.acked << ",\"pending\":" << st.pending
             << ",\"in_flight\":" << st.in_flight << ",\"dead\":" << st.dead
             << ",\"rebuilds\":" << m.producer_rebuilds << ",\"delivery_fail\":" << m.delivery_fail
             << ",\"state_errors\":" << m.state_errors << "}\n";
      status.flush();
      if (done && st.pending == 0 && st.in_flight == 0 && st.dead == 0) {
        drained = true;
        break;
      }
      if (done && (mode == "poison" || mode == "ttl") &&
          Clock::now() - began > std::chrono::seconds(12))
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (generator.joinable()) generator.join();
    p.stop();
    snapshot(root);
    auto& m = p.metrics();
    std::ofstream summary(root / (resume ? "sender-resume.json" : "sender.json"));
    summary << "{\"pid\":" << getpid() << ",\"mode\":\"" << mode << "\",\"librdkafka\":\""
            << rd_kafka_version_str() << "\",\"enqueued\":" << m.enqueued
            << ",\"acked\":" << m.acked << ",\"delivery_fail\":" << m.delivery_fail
            << ",\"produce_fail\":" << m.produce_fail
            << ",\"permanent_errors\":" << m.permanent_errors
            << ",\"state_errors\":" << m.state_errors << ",\"rebuilds\":" << m.producer_rebuilds
            << ",\"rejected\":" << rejected << ",\"peak_rows\":" << peak_rows
            << ",\"peak_payload_bytes\":" << peak_bytes << ",\"duration_ms\":"
            << std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - began).count()
            << ",\"drained\":" << (drained ? "true" : "false") << "}\n";
    return rejected || m.state_errors ? 3 : drained || mode == "poison" || mode == "ttl" ? 0 : 4;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 2;
  }
}
