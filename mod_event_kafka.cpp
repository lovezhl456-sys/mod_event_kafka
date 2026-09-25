/*
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * Copyright (C) 2005-2014, Anthony Minessale II <anthm@freeswitch.org>
 *
 * Version: MPL 1.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 *
 * The Initial Developer of the Original Code is
 * Anthony Minessale II <anthm@freeswitch.org>
 * Portions created by the Initial Developer are Copyright (C)
 * the Initial Developer. All Rights Reserved.
 *
 * Based on mod_skel by
 * Anthony Minessale II <anthm@freeswitch.org>
 *
 * Contributor(s):
 * 
 * Kinshuk Bairagi <me@kinshuk.in>
 *
 * mod_event_kafka.c -- Sends FreeSWITCH events to an Kafka broker
 *
 */

#include <iostream>
#include <string>
#include <thread>
#include <memory>
#include <switch.h>
#include "mod_event_kafka.hpp"
#include "kafka_pipeline.hpp"

namespace mod_event_kafka {

    template <typename T, std::size_t Muliplier = 1>
    T* malloc_new()
    {
        return static_cast<T*>(std::malloc(sizeof(T) * Muliplier));
    }

    static switch_xml_config_item_t instructions[] = {
        SWITCH_CONFIG_ITEM("bootstrap-servers", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.brokers,
                            "localhost:9092", NULL, "bootstrap-servers", "Kafka Bootstrap Brokers"),
        SWITCH_CONFIG_ITEM("username", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.username, "", NULL, "username", "Username"),
        SWITCH_CONFIG_ITEM("password", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.password, "", NULL, "password", "Password"),
        SWITCH_CONFIG_ITEM("topic", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.topic,
                            "", NULL, "topic", "Kafka Topic"),
        SWITCH_CONFIG_ITEM("topic-prefix", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.topic_prefix,
                            "fs", NULL, "topic-prefix", "Kafka Topic Prefix"),
        SWITCH_CONFIG_ITEM("buffer-size", SWITCH_CONFIG_INT, CONFIG_RELOADABLE, &globals.buffer_size,
                            10, NULL, "buffer-size", "queue.buffering.max.messages"),
        SWITCH_CONFIG_ITEM("compression", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.compression,
                            "snappy", NULL, "snappy / lz4 ", "Compression"),
        SWITCH_CONFIG_ITEM("event-filter", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.event_filter,
                            "", NULL, "comma separated value of event names", "Event Filter"),

        SWITCH_CONFIG_ITEM("outbox-path", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.outbox_path,
                            "/var/lib/freeswitch/event_kafka_outbox.db", NULL, "outbox-path", "SQLite outbox path"),
        SWITCH_CONFIG_ITEM("mem-queue-max", SWITCH_CONFIG_INT, CONFIG_RELOADABLE, &globals.mem_queue_max,
                            10000, NULL, "mem-queue-max", "Bounded in-memory queue depth"),
        SWITCH_CONFIG_ITEM("outbox-max-rows", SWITCH_CONFIG_INT, CONFIG_RELOADABLE, &globals.outbox_max_rows,
                            100000, NULL, "outbox-max-rows", "Max durable outbox rows"),
        SWITCH_CONFIG_ITEM("message-timeout-ms", SWITCH_CONFIG_INT, CONFIG_RELOADABLE, &globals.message_timeout_ms,
                            30000, NULL, "message-timeout-ms", "Topic message.timeout.ms (must be applied)"),
        SWITCH_CONFIG_ITEM("enable-idempotence", SWITCH_CONFIG_INT, CONFIG_RELOADABLE, &globals.enable_idempotence,
                            1, NULL, "enable-idempotence", "librdkafka enable.idempotence"),
        SWITCH_CONFIG_ITEM("security-protocol", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.security_protocol,
                            "", NULL, "security-protocol", "PLAINTEXT/SASL_PLAINTEXT/SASL_SSL/SSL"),
        SWITCH_CONFIG_ITEM("ssl-ca-location", SWITCH_CONFIG_STRING, CONFIG_RELOADABLE, &globals.ssl_ca_location,
                            "", NULL, "ssl-ca-location", "ssl.ca.location"),
        SWITCH_CONFIG_ITEM_END()
    };

    static switch_status_t load_config(switch_bool_t reload)
    {
        memset(&globals, 0, sizeof(globals));
        if (switch_xml_config_parse_module_settings("event_kafka.conf", reload, instructions) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Could not open event_kafka.conf\n");
            return SWITCH_STATUS_FALSE;
        } else {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "event_kafka.conf loaded [brokers: %s, topic/topic_prefix: %s/%s, username: %s, buffer-size: %d, compression: %s]", 
            globals.brokers, globals.topic, globals.topic_prefix, globals.username, globals.buffer_size, globals.compression);
        }
        return SWITCH_STATUS_SUCCESS;
    }



    class KafkaEventPublisher {
        public:
        KafkaEventPublisher() {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "KafkaEventPublisher Initialising (outbox pipeline)...");
            load_config(SWITCH_FALSE);

            if (globals.topic && globals.topic[0] == '\0') {
                std::string topic_str = std::string(globals.topic_prefix) + "_" + std::string(switch_core_get_switchname());
                strcpy(globals.topic, topic_str.c_str());
            }
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "KafkaEventPublisher Topic : %s \n", globals.topic);

            event_kafka::PipelineConfig cfg;
            cfg.brokers = globals.brokers ? globals.brokers : "localhost:9092";
            cfg.topic = globals.topic ? globals.topic : "fs_events";
            cfg.username = globals.username ? globals.username : "";
            cfg.password = globals.password ? globals.password : "";
            cfg.security_protocol = globals.security_protocol ? globals.security_protocol : "";
            cfg.ssl_ca_location = globals.ssl_ca_location ? globals.ssl_ca_location : "";
            cfg.compression = globals.compression ? globals.compression : "snappy";
            cfg.outbox_path = globals.outbox_path ? globals.outbox_path : "/var/lib/freeswitch/event_kafka_outbox.db";
            cfg.buffer_size = globals.buffer_size > 0 ? globals.buffer_size : 100000;
            cfg.mem_queue_max = static_cast<size_t>(globals.mem_queue_max > 0 ? globals.mem_queue_max : 10000);
            cfg.outbox_max_rows = globals.outbox_max_rows > 0 ? globals.outbox_max_rows : 100000;
            cfg.message_timeout_ms = globals.message_timeout_ms > 0 ? globals.message_timeout_ms : 30000;
            cfg.enable_idempotence = globals.enable_idempotence != 0;

            pipeline_.reset(new event_kafka::KafkaPipeline(cfg));
            std::string err;
            if (!pipeline_->start(err)) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "KafkaPipeline start failed: %s\n", err.c_str());
                _initialized = false;
                return;
            }
            _initialized = true;
        }

        void PublishEvent(switch_event_t *event) {
            char *uuid = switch_event_get_header(event, "Channel-Call-UUID");
            char *event_json = malloc_new<char>();
            const switch_status_t json_status = switch_event_serialize_json(event, &event_json);
            if (json_status == SWITCH_STATUS_FALSE) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "json serialization failed in switch\n");
                std::free(event_json);
                return;
            }

            if (!_initialized || !pipeline_) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "PublishEvent without active KafkaPublisher\n %s \n", event_json);
                std::free(event_json);
                return;
            }

            // Deep-copy into pipeline (payload string ctor copies); free FS buffer after enqueue attempt.
            std::string payload(event_json);
            std::string key = uuid ? std::string(uuid) : std::string();
            std::string call_uuid = key;
            std::string event_id;
            std::string err;
            const bool ok = pipeline_->enqueue(payload, key, call_uuid, event_id, err);
            std::free(event_json);
            if (!ok) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                                  "event enqueue rejected (%s); metrics rejected_mem=%llu\n",
                                  err.c_str(),
                                  (unsigned long long)pipeline_->metrics().rejected_mem_full.load());
            } else {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
                                  "enqueued event_id=%s key=%s\n", event_id.c_str(), key.c_str());
            }
        }

        void Shutdown() {
            if (pipeline_) pipeline_->stop();
        }

        ~KafkaEventPublisher() {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "KafkaEventPublisher Destroyed\n");
            if (pipeline_) {
                pipeline_->stop();
                pipeline_.reset();
            }
        }

        private:
        bool _initialized = false;
        std::unique_ptr<event_kafka::KafkaPipeline> pipeline_;
    };

    class KafkaModule {
    public:

        KafkaModule(switch_loadable_module_interface_t **module_interface, switch_memory_pool_t *pool): _publisher() {
             
            char *event_filter_name[SWITCH_EVENT_ALL];
            char *switch_event_custom = (char*) std::string("SWITCH_EVENT_CUSTOM::").c_str();
            profile.event_subscriptions = switch_separate_string(globals.event_filter, ',', event_filter_name, (sizeof(event_filter_name) / sizeof(event_filter_name[0])));
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Found %d subscriptions\n", profile.event_subscriptions);
            for (int i = 0; i < profile.event_subscriptions; i++) {
                if (switch_name_event(event_filter_name[i], &(profile.event_ids[i])) != SWITCH_STATUS_SUCCESS && !switch_strstr(event_filter_name[i],switch_event_custom) ) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "The switch event %s was not recognised.\n", event_filter_name[i]);
                } else {
                    // switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Found subscription for %s event.\n", argv[arg]);
                }
            }

            if (profile.event_subscriptions > 0 ) {
                /* Subscribe events */
                for (int i = 0; i < profile.event_subscriptions; i++) {
                    if ( switch_strstr(event_filter_name[i], switch_event_custom)) {
                        if (switch_event_bind_removable(modname, SWITCH_EVENT_CUSTOM, event_filter_name[i] + strlen("SWITCH_EVENT_CUSTOM::"),
                                                        event_handler, static_cast<void*>(&_publisher),&(profile.event_nodes[i])) != SWITCH_STATUS_SUCCESS) {
                            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Cannot bind to event handler %d!\n",(int)profile.event_ids[i]);
                            throw std::invalid_argument("Failed to bind event handler for " + std::string(event_filter_name[i]));
                        }
                    } else {
                        if (switch_event_bind_removable(modname, profile.event_ids[i], SWITCH_EVENT_SUBCLASS_ANY,
                                                        event_handler, static_cast<void*>(&_publisher), &(profile.event_nodes[i])) != SWITCH_STATUS_SUCCESS) {
                            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Cannot bind to event handler %d!\n",(int)profile.event_ids[i]);
                            throw std::invalid_argument( "Failed to bind event handler for " + std::string(event_filter_name[i]));
                        }
                    }
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Subscribed to %s event.\n", event_filter_name[i]);
                }

            } else {
                // Subscribe to all switch events of any subclass
                // Store a pointer to ourself in the user data
                if (switch_event_bind_removable(modname, SWITCH_EVENT_ALL, SWITCH_EVENT_SUBCLASS_ANY, event_handler,
                                                static_cast<void*>(&_publisher), &_node)
                    != SWITCH_STATUS_SUCCESS) {
                    throw std::runtime_error("Couldn't bind to switch events.");
                }
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Subscribed to ALL events\n");
                
            }


            // Create our module interface registration
            *module_interface = switch_loadable_module_create_module_interface(pool, modname);

            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Module loaded completed\n");

        };

        void Shutdown() {
            // Send term message
            _publisher.Shutdown();
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Shutdown requested, flushing publisher\n");
        }

        ~KafkaModule() {
            // Unsubscribe from the switch events
            if (profile.event_subscriptions > 0 ) {
                for (int i = 0; i < profile.event_subscriptions; i++) {
                    switch_event_unbind(&(profile.event_nodes[i]));
                }
            } else {
                switch_event_unbind(&_node);
            }
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Module shut down\n");
        }

    private:

        // Dispatches events to the publisher
        static void event_handler(switch_event_t *event) {
            try {
                KafkaEventPublisher *publisher = static_cast<KafkaEventPublisher*>(event->bind_user_data);
                publisher->PublishEvent(event);
            } catch (std::exception const &ex) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Error publishing event to Kafka: %s\n",
                                  ex.what());
            } catch (...) { // Exceptions must not propogate to C caller
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Unknown error publishing event to Kafka\n");
            }
        }

        switch_event_node_t *_node;
        KafkaEventPublisher _publisher;

    };


    //*****************************//
    //           GLOBALS           //
    //*****************************//
    std::unique_ptr<KafkaModule> module;


    //*****************************//
    //  Module interface funtions  //
    //*****************************//
    SWITCH_MODULE_LOAD_FUNCTION(mod_event_kafka_load) {
            try {
                module = std::make_unique<KafkaModule>(module_interface, pool);
                return SWITCH_STATUS_SUCCESS;
            } catch(...) { // Exceptions must not propogate to C caller
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Error loading Kafka Event module\n");
                return SWITCH_STATUS_GENERR;
            }

    }


    SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_event_kafka_shutdown) {
            try {
                // Tell the module to shutdown
                module->Shutdown();
                // Free the module object
                module.reset();
            } catch(std::exception const &ex) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Error shutting down Kafka Event module: %s\n",
                                  ex.what());
            } catch(...) { // Exceptions must not propogate to C caller
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Unknown error shutting down Kafka Event module\n");
            }
            return SWITCH_STATUS_SUCCESS;
    }

}

