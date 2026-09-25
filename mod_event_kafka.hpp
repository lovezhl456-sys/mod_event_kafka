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
		/* Additive resilience settings (defaults preserve prior behavior dimensions) */
		char *outbox_path;
		char *security_protocol;
		char *ssl_ca_location;
		int mem_queue_max;
		int outbox_max_rows;
		int message_timeout_ms;
		int enable_idempotence;
	} globals;


	static struct {
		/* Array to store the possible event subscriptions */
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
