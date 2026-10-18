package com.example.kafka.inventory.consumer;

import com.example.kafka.avro.InventoryResultEvent;
import com.example.kafka.avro.OrderEvent;
import com.example.kafka.avro.OrderItem;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.entity.ProcessedEvent;
import com.example.kafka.common.outbox.OutboxWriter;
import com.example.kafka.common.repository.ProcessedEventRepository;
import com.example.kafka.inventory.entity.Stock;
import com.example.kafka.inventory.repository.StockRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final StockRepository stockRepository;
    private final ProcessedEventRepository processedEvents;
    private final OutboxWriter outbox;

    public OrderEventConsumer(StockRepository stockRepository,
                              ProcessedEventRepository processedEvents,
                              OutboxWriter outbox) {
        this.stockRepository = stockRepository;
        this.processedEvents = processedEvents;
        this.outbox = outbox;
    }

    /**
     * Idempotent consumer + saga participant. The stock change, the processed-event marker and the
     * result event (via the outbox) commit in one DB transaction; the offset is committed by the
     * container after this method returns, so a crash at any point leads to a safe redelivery.
     */
    @KafkaListener(topics = KafkaTopics.ORDERS_EVENTS)
    @Transactional
    public void onOrderEvent(OrderEvent event) {
        if (processedEvents.existsById(event.getEventId())) {
            log.info("Duplicate event {} for order {}, skipped", event.getEventId(), event.getOrderId());
            return;
        }

        String failure = reserve(event.getItems());

        InventoryResultEvent result = InventoryResultEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(failure == null ? "INVENTORY_RESERVED" : "INVENTORY_FAILED")
                .setOccurredAt(Instant.now().toEpochMilli())
                .setAggregateId(event.getOrderId())
                .setOrderId(event.getOrderId())
                .setSuccess(failure == null)
                .setReason(failure)
                .build();

        outbox.enqueue(KafkaTopics.INVENTORY_EVENTS, event.getOrderId(), result);
        processedEvents.save(new ProcessedEvent(event.getEventId()));
        log.info("Order {}: {}", event.getOrderId(), result.getEventType());
    }

    /** All-or-nothing reservation. Returns null on success, the failure reason otherwise. */
    private String reserve(List<OrderItem> items) {
        Map<String, Integer> requested = items.stream()
                .collect(Collectors.toMap(OrderItem::getProductId, OrderItem::getQuantity, Integer::sum));
        Map<String, Stock> stocks = stockRepository.lockAll(requested.keySet()).stream()
                .collect(Collectors.toMap(Stock::getProductId, Function.identity()));

        for (var line : requested.entrySet()) {
            Stock stock = stocks.get(line.getKey());
            if (stock == null) {
                return "Unknown product: " + line.getKey();
            }
            if (!stock.canReserve(line.getValue())) {
                return "Insufficient stock for product: " + line.getKey();
            }
        }
        requested.forEach((productId, quantity) -> stocks.get(productId).reserve(quantity));
        return null;
    }
}
