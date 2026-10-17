package com.example.kafka.common.constant;

public final class KafkaTopics {

    private KafkaTopics() {}

    public static final String ORDERS_EVENTS = "orders.events";
    public static final String INVENTORY_EVENTS = "inventory.events";
    public static final String NOTIFICATIONS_EVENTS = "notifications.events";
    
    // Topics de résilience (Retry & Dead Letter Topic)
    public static final String ORDERS_RETRY = "orders.retry";
    public static final String ORDERS_DLT = "orders.dlt";
}
