package com.example.kafka.common.constant;

/**
 * Topic names. All topics (including -dlt and retry topics) are created by the kafka-init container
 * in docker-compose.yml; brokers have auto-creation disabled.
 */
public final class KafkaTopics {

    private KafkaTopics() {}

    public static final String ORDERS_EVENTS = "orders.events";
    public static final String INVENTORY_EVENTS = "inventory.events";
}
