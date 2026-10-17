package com.example.kafka.order.consumer;

import com.example.kafka.avro.InventoryResultEvent;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.order.entity.Order;
import com.example.kafka.order.service.OrderService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryResultConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryResultConsumer.class);

    private final OrderService orderService;

    public InventoryResultConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Consomme les résultats d'inventaire pour confirmer ou rejeter la commande (Saga chorégraphiée).
     */
    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS, groupId = "order-service-inventory-group")
    public void onInventoryResult(ConsumerRecord<String, InventoryResultEvent> record) {
        InventoryResultEvent result = record.value();
        log.info("Réception résultat inventaire pour commande {} : succès={}", result.getOrderId(), result.getSuccess());

        if (result.getSuccess()) {
            orderService.updateOrderStatus(result.getOrderId(), Order.OrderStatus.CONFIRMED);
        } else {
            orderService.updateOrderStatus(result.getOrderId(), Order.OrderStatus.REJECTED);
        }
    }
}
