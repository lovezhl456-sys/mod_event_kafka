// Independent consumer, after producer exits. Read exactly the broker's end offsets.
#include <librdkafka/rdkafka.h>

#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
int main(int argc, char** argv) {
  try {
    if (argc != 3) throw std::runtime_error("TOPIC OUTPUT");
    char err[512];
    auto* cfg = rd_kafka_conf_new();
    rd_kafka_conf_set(cfg, "bootstrap.servers", "127.0.0.1:39092", err, sizeof err);
    rd_kafka_conf_set(cfg, "group.id", "order-independent-readback", err, sizeof err);
    rd_kafka_conf_set(cfg, "enable.auto.commit", "false", err, sizeof err);
    auto* c = rd_kafka_new(RD_KAFKA_CONSUMER, cfg, err, sizeof err);
    if (!c) throw std::runtime_error(err);
    rd_kafka_poll_set_consumer(c);
    auto* ps = rd_kafka_topic_partition_list_new(3);
    std::map<int, int64_t> ends;
    int64_t remaining = 0;
    for (int part = 0; part < 3; part++) {
      int64_t low, high;
      auto rc = rd_kafka_query_watermark_offsets(c, argv[1], part, &low, &high, 20000);
      if (rc) throw std::runtime_error(rd_kafka_err2str(rc));
      std::cout << part << ' ' << low << ' ' << high << '\n';
      remaining += high - low;
      ends[part] = low;
      if (high > low) rd_kafka_topic_partition_list_add(ps, argv[1], part)->offset = low;
    }
    if (rd_kafka_assign(c, ps)) throw std::runtime_error("assign");
    rd_kafka_topic_partition_list_destroy(ps);
    std::ofstream out(argv[2]);
    while (remaining) {
      auto* m = rd_kafka_consumer_poll(c, 20000);
      if (!m) throw std::runtime_error("read timeout");
      if (m->err) throw std::runtime_error(rd_kafka_message_errstr(m));
      if (m->offset != ends[m->partition]++) throw std::runtime_error("offset gap");
      rd_kafka_headers_t* h = nullptr;
      const void *id = nullptr, *seq = nullptr;
      size_t il = 0, sl = 0;
      rd_kafka_message_headers(m, &h);
      if (!h || rd_kafka_header_get_last(h, "x-fs-event-id", &id, &il))
        throw std::runtime_error("event ID missing");
      rd_kafka_header_get_last(h, "x-fs-call-sequence", &seq, &sl);
      out << m->partition << '\t' << m->offset << '\t'
          << std::string(static_cast<const char*>(id), il) << '\t'
          << (seq ? std::string(static_cast<const char*>(seq), sl) : "") << '\t'
          << std::string(static_cast<char*>(m->key), m->key_len) << '\t'
          << std::string(static_cast<char*>(m->payload), m->len) << '\n';
      --remaining;
      rd_kafka_message_destroy(m);
    }
    rd_kafka_consumer_close(c);
    rd_kafka_destroy(c);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
