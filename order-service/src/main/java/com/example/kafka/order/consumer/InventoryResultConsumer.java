package com.example.kafka.order.consumer;

import com.example.kafka.avro.InventoryResultEvent;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.order.service.OrderService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryResultConsumer {

    private final OrderService orderService;

    public InventoryResultConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS)
    public void onInventoryResult(InventoryResultEvent result) {
        orderService.applyInventoryResult(result);
    }
}
