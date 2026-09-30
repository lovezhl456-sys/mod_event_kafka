#ifndef MOD_EVENT_KAFKA_H
#define MOD_EVENT_KAFKA_H

extern "C" {
	#include "librdkafka/rdkafka.h"
}

namespace mod_event_kafka {

	static struct {
		char *brokers;
		char *topic_prefix;
		char *topic;
		char *username;
		char *password;
		int buffer_size;
		char *compression;
		char *event_filter;
		/* 新增的可靠性配置（默认值保持原有行为） */
		char *outbox_path;
		char *security_protocol;
		char *ssl_ca_location;
		int mem_queue_max;
		int outbox_max_rows;
		int message_timeout_ms;
		int enable_idempotence;
		/* 0 表示关闭过期；解析配置时套用的默认值为 120000。 */
		int outbox_ttl_ms;
	} globals;


	static struct {
		/* 存放各项事件订阅的数组 */
		int event_subscriptions;
		switch_event_node_t *event_nodes[SWITCH_EVENT_ALL];
		switch_event_types_t event_ids[SWITCH_EVENT_ALL];
		switch_event_node_t *eventNode;
	} profile;

	SWITCH_MODULE_LOAD_FUNCTION(mod_event_kafka_load);
	SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_event_kafka_shutdown);

	extern "C" {
		SWITCH_MODULE_DEFINITION(mod_event_kafka, mod_event_kafka_load, mod_event_kafka_shutdown, NULL);
	};
};
#endif // MOD_EVENT_KAFKA_H
