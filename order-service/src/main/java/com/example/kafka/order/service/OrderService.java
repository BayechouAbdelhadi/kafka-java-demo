package com.example.kafka.order.service;

import com.example.kafka.avro.OrderEvent;
import com.example.kafka.avro.OrderItem;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.entity.OutboxMessage;
import com.example.kafka.common.repository.OutboxMessageRepository;
import com.example.kafka.order.dto.CreateOrderRequest;
import com.example.kafka.order.entity.Order;
import com.example.kafka.order.entity.OrderItemEntity;
import com.example.kafka.order.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxMessageRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxMessageRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Crée une commande et insère l'événement dans la table Outbox au sein
     * de la MÊME TRANSACTION LOCALE PostgreSQL (Transactional Outbox Pattern).
     */
    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        double total = request.getItems().stream()
                .mapToDouble(i -> i.getQuantity() * i.getUnitPrice())
                .sum();

        Order order = new Order(request.getCustomerId(), total);
        for (CreateOrderRequest.ItemRequest itemReq : request.getItems()) {
            order.addItem(new OrderItemEntity(itemReq.getProductId(), itemReq.getQuantity(), itemReq.getUnitPrice()));
        }

        // 1. Sauvegarde locale de l'agrégat Order
        Order savedOrder = orderRepository.save(order);
        log.info("Commande {} enregistrée en base (statut: {})", savedOrder.getId(), savedOrder.getStatus());

        // 2. Construction de l'événement de domaine Avro
        String eventId = UUID.randomUUID().toString();
        List<OrderItem> avroItems = savedOrder.getItems().stream()
                .map(i -> new OrderItem(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
                .collect(Collectors.toList());

        OrderEvent event = OrderEvent.newBuilder()
                .setEventId(eventId)
                .setEventType("ORDER_CREATED")
                .setOccurredAt(Instant.now().toEpochMilli())
                .setAggregateId(savedOrder.getId())
                .setOrderId(savedOrder.getId())
                .setCustomerId(savedOrder.getCustomerId())
                .setTotalAmount(savedOrder.getTotalAmount())
                .setStatus(savedOrder.getStatus().name())
                .setItems(avroItems)
                .build();

        // 3. Enregistrement dans la table outbox_messages au sein de la même transaction
        try {
            // Sérialisation JSON pour stockage temporaire dans l'outbox
            String payloadJson = objectMapper.writeValueAsString(event);
            OutboxMessage outbox = new OutboxMessage(
                    "Order",
                    savedOrder.getId(),
                    "ORDER_CREATED",
                    KafkaTopics.ORDERS_EVENTS,
                    savedOrder.getId(), // Clé de partitionnement : orderId
                    payloadJson
            );
            outboxRepository.save(outbox);
            log.info("Événement Outbox {} créé atomiquement pour la commande {}", outbox.getId(), savedOrder.getId());
        } catch (Exception e) {
            log.error("Erreur lors de la sérialisation de l'événement Outbox", e);
            throw new RuntimeException("Échec d'enregistrement de l'outbox", e);
        }

        return savedOrder;
    }

    @Transactional(readOnly = true)
    public Order getOrder(String id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Commande introuvable: " + id));
    }

    @Transactional(readOnly = true)
    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    @Transactional
    public void updateOrderStatus(String orderId, Order.OrderStatus newStatus) {
        orderRepository.findById(orderId).ifPresent(order -> {
            order.setStatus(newStatus);
            orderRepository.save(order);
            log.info("Statut de la commande {} mis à jour : {}", orderId, newStatus);
        });
    }
}
