#include "kafka_pipeline.hpp"
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main(int argc, char** argv) {
  const char* brokers = argc > 1 ? argv[1] : "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094";
  const char* topic = argc > 2 ? argv[2] : "fs_events";
  event_kafka::PipelineConfig cfg;
  cfg.brokers = brokers;
  cfg.topic = topic;
  cfg.outbox_path = "/tmp/event_kafka_pipeline_smoke.db";
  cfg.mem_queue_max = 1000;
  cfg.outbox_max_rows = 10000;
  cfg.message_timeout_ms = 30000;
  std::string err;
  event_kafka::KafkaPipeline pipe(cfg);
  if (!pipe.start(err)) {
    std::cerr << "start failed: " << err << "\n";
    return 2;
  }
  for (int i = 0; i < 20; ++i) {
    std::string eid, e;
    std::string payload = std::string("{\"Event-Name\":\"TEST\",\"i\":") + std::to_string(i) + "}";
    if (!pipe.enqueue(payload, "call-smoke", "call-smoke", eid, e)) {
      std::cerr << "enqueue fail: " << e << "\n";
      return 3;
    }
  }
  for (int i = 0; i < 150; ++i) {
    if (pipe.metrics().acked.load() >= 20) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  const auto enq = pipe.metrics().enqueued.load();
  const auto pok = pipe.metrics().produce_ok.load();
  const auto acked = pipe.metrics().acked.load();
  const auto df = pipe.metrics().delivery_fail.load();
  std::cout << "enqueued=" << enq << " produce_ok=" << pok << " acked=" << acked
            << " delivery_fail=" << df << std::endl;
  pipe.stop();
  const bool ok = acked >= 20 && pok >= 20;
  std::cout << "final_acked=" << acked << " final_produce_ok=" << pok << " ok=" << (ok ? 1 : 0)
            << std::endl;
  std::cout << (ok ? "SMOKE_PASS\n" : "SMOKE_FAIL_OR_LAB_DOWN\n");
  return ok ? 0 : 4;
}
