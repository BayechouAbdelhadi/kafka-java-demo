package com.example.kafka.inventory.consumer;

import com.example.kafka.avro.InventoryResultEvent;
import com.example.kafka.avro.OrderEvent;
import com.example.kafka.avro.OrderItem;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.entity.ProcessedEvent;
import com.example.kafka.common.repository.ProcessedEventRepository;
import com.example.kafka.inventory.entity.Stock;
import com.example.kafka.inventory.repository.StockRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private static final String CONSUMER_GROUP = "inventory-service-group";

    private final StockRepository stockRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventConsumer(StockRepository stockRepository,
                              ProcessedEventRepository processedEventRepository,
                              KafkaTemplate<String, Object> kafkaTemplate) {
        this.stockRepository = stockRepository;
        this.processedEventRepository = processedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Consommateur idempotent :
     * 1. Vérifie si l'événement a déjà été traité (table processed_events).
     * 2. Si doublon détecté -> ignore l'effet métier (pas de double décrément de stock).
     * 3. Si nouveau -> réserve le stock et enregistre l'idempotence dans la même transaction.
     * 4. Publie le résultat sur inventory.events.
     */
    @KafkaListener(topics = KafkaTopics.ORDERS_EVENTS, groupId = CONSUMER_GROUP)
    @Transactional
    public void handleOrderEvent(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        OrderEvent event = record.value();
        String eventId = event.getEventId();

        log.info("Inventory : Réception événement {} pour commande {} (partition: {}, offset: {})",
                eventId, event.getOrderId(), record.partition(), record.offset());

        // --- ÉTAPE IDEMPOTENCE MÉTIER ---
        if (processedEventRepository.existsById(eventId)) {
            log.warn("⚠️ [DÉDUPLICATION] Événement {} déjà traité ! Action métier ignorée pour éviter tout double débit.", eventId);
            if (ack != null) ack.acknowledge();
            return;
        }

        boolean allReserved = true;
        String failureReason = null;

        for (OrderItem item : event.getItems()) {
            Stock stock = stockRepository.findById(item.getProductId())
                    .orElseGet(() -> stockRepository.save(new Stock(item.getProductId(), 100))); // Init par défaut

            if (!stock.reserve(item.getQuantity())) {
                allReserved = false;
                failureReason = "Stock insuffisant pour le produit: " + item.getProductId();
                break;
            }
            stockRepository.save(stock);
        }

        // Enregistrement de l'idempotence au sein de la transaction locale
        processedEventRepository.save(new ProcessedEvent(eventId, event.getEventType(), CONSUMER_GROUP));

        // Publication de l'événement de résultat
        InventoryResultEvent resultEvent = InventoryResultEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(allReserved ? "INVENTORY_RESERVED" : "INVENTORY_FAILED")
                .setOccurredAt(Instant.now().toEpochMilli())
                .setAggregateId(event.getOrderId())
                .setOrderId(event.getOrderId())
                .setSuccess(allReserved)
                .setReason(failureReason)
                .build();

        kafkaTemplate.send(KafkaTopics.INVENTORY_EVENTS, event.getOrderId(), resultEvent);
        log.info("Inventory : Résultat envoyé pour commande {} (succès: {})", event.getOrderId(), allReserved);

        // Commit manuel de l'offset Kafka après confirmation transactionnelle
        if (ack != null) {
            ack.acknowledge();
        }
    }
}
