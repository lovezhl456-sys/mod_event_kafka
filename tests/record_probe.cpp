// Protocol/error-code probe. Deliberately separate from the FS/outbox pipeline.
#include <librdkafka/rdkafka.h>
#include <cstdlib>
#include <cstring>
#include <iostream>

struct Result { int delivered{0}; int unexpected{0}; int expected{0}; };
static void delivered(rd_kafka_t*, const rd_kafka_message_t* msg, void* opaque) {
  auto* r = static_cast<Result*>(opaque);
  ++r->delivered;
  if (static_cast<int>(msg->err) != r->expected) ++r->unexpected;
  std::cout << "DR code=" << static_cast<int>(msg->err) << " reason=" << rd_kafka_err2str(msg->err)
            << " partition=" << msg->partition << " offset=" << msg->offset << '\n';
}
int main(int argc, char** argv) {
  if (argc != 6) {
    std::cerr << "usage: record_probe BROKERS TOPIC null|key COMPRESSION EXPECTED_CODE\n";
    return 2;
  }
  if (std::strcmp(argv[3], "null") && std::strcmp(argv[3], "key")) return 2;
  Result result;
  result.expected = std::atoi(argv[5]);
  char err[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  const char* names[] = {"bootstrap.servers", "compression.codec", "message.timeout.ms", "debug"};
  const char* values[] = {argv[1], argv[4], "5000", "broker,protocol,feature"};
  for (int i = 0; i < 4; ++i) {
    if (rd_kafka_conf_set(conf, names[i], values[i], err, sizeof(err)) != RD_KAFKA_CONF_OK) {
      std::cerr << names[i] << ": " << err << '\n';
      rd_kafka_conf_destroy(conf);
      return 2;
    }
  }
  std::cout << "librdkafka=" << rd_kafka_version_str() << '\n';
  for (const auto* key : {"api.version.request", "api.version.fallback.ms", "broker.version.fallback"}) {
    char value[128]; size_t size = sizeof(value);
    if (rd_kafka_conf_get(conf, key, value, &size) == RD_KAFKA_CONF_OK)
      std::cout << key << '=' << value << '\n';
  }
  rd_kafka_conf_set_opaque(conf, &result);
  rd_kafka_conf_set_dr_msg_cb(conf, delivered);
  auto* producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, err, sizeof(err));
  if (!producer) { std::cerr << err << '\n'; return 2; }
  auto* topic = rd_kafka_topic_new(producer, argv[2], nullptr);
  if (!topic) { rd_kafka_destroy(producer); return 2; }
  const char* key = std::strcmp(argv[3], "null") ? "probe-key" : nullptr;
  for (int i = 0; i < 5; ++i) {
    const char* payload = "record-validation-probe";
    if (rd_kafka_produce(topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
          const_cast<char*>(payload), std::strlen(payload), key, key ? std::strlen(key) : 0, nullptr)) {
      std::cerr << "ENQUEUE_ERROR " << rd_kafka_err2str(rd_kafka_last_error()) << '\n';
      ++result.unexpected;
    }
  }
  const auto flush_error = rd_kafka_flush(producer, 10000);
  rd_kafka_topic_destroy(topic);
  rd_kafka_destroy(producer);
  const bool ok = !flush_error && result.delivered == 5 && result.unexpected == 0;
  std::cout << "PROBE_" << (ok ? "PASS" : "FAIL") << " callbacks=" << result.delivered << '\n';
  return ok ? 0 : 1;
}
