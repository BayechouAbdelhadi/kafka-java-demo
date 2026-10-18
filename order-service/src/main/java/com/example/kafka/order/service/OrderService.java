package com.example.kafka.order.service;

import com.example.kafka.avro.InventoryResultEvent;
import com.example.kafka.avro.OrderEvent;
import com.example.kafka.avro.OrderItem;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.outbox.OutboxWriter;
import com.example.kafka.order.dto.CreateOrderRequest;
import com.example.kafka.order.entity.Order;
import com.example.kafka.order.entity.OrderItemEntity;
import com.example.kafka.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxWriter outbox;

    public OrderService(OrderRepository orderRepository, OutboxWriter outbox) {
        this.orderRepository = orderRepository;
        this.outbox = outbox;
    }

    /** Saves the order and its ORDER_CREATED event in the same local transaction (transactional outbox). */
    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        double total = request.items().stream().mapToDouble(i -> i.quantity() * i.unitPrice()).sum();

        Order order = new Order(request.customerId(), total);
        request.items().forEach(i -> order.addItem(new OrderItemEntity(i.productId(), i.quantity(), i.unitPrice())));
        orderRepository.save(order);

        List<OrderItem> items = order.getItems().stream()
                .map(i -> new OrderItem(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
                .toList();

        OrderEvent event = OrderEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("ORDER_CREATED")
                .setOccurredAt(Instant.now().toEpochMilli())
                .setAggregateId(order.getId())
                .setOrderId(order.getId())
                .setCustomerId(order.getCustomerId())
                .setTotalAmount(order.getTotalAmount())
                .setStatus(order.getStatus().name())
                .setItems(items)
                .build();

        // Key = orderId: every event of an order lands on the same partition, in order.
        outbox.enqueue(KafkaTopics.ORDERS_EVENTS, order.getId(), event);
        log.info("Order {} created", order.getId());
        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrder(String id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found: " + id));
    }

    @Transactional(readOnly = true)
    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    /**
     * Saga step: confirm or reject the order. Idempotent through the state machine —
     * only a CREATED order can transition, so a redelivered result is a no-op.
     */
    @Transactional
    public void applyInventoryResult(InventoryResultEvent result) {
        orderRepository.findById(result.getOrderId()).ifPresentOrElse(order -> {
            if (order.getStatus() != Order.OrderStatus.CREATED) {
                log.info("Order {} already {}, ignoring {}", order.getId(), order.getStatus(), result.getEventType());
                return;
            }
            order.setStatus(result.getSuccess() ? Order.OrderStatus.CONFIRMED : Order.OrderStatus.REJECTED);
            log.info("Order {} -> {}{}", order.getId(), order.getStatus(),
                    result.getReason() == null ? "" : " (" + result.getReason() + ")");
        }, () -> log.warn("Inventory result for unknown order {}", result.getOrderId()));
    }
}
