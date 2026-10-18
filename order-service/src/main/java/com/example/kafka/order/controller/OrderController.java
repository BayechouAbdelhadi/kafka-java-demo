package com.example.kafka.order.controller;

import com.example.kafka.common.repository.OutboxMessageRepository;
import com.example.kafka.order.dto.CreateOrderRequest;
import com.example.kafka.order.entity.Order;
import com.example.kafka.order.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    public record PendingOutboxMessage(Long id, String topic, String key, String type, Instant createdAt) {}

    private final OrderService orderService;
    private final OutboxMessageRepository outboxRepository;

    public OrderController(OrderService orderService, OutboxMessageRepository outboxRepository) {
        this.orderService = orderService;
        this.outboxRepository = outboxRepository;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Order createOrder(@Valid @RequestBody CreateOrderRequest request) {
        return orderService.createOrder(request);
    }

    @GetMapping("/{id}")
    public Order getOrder(@PathVariable String id) {
        return orderService.getOrder(id);
    }

    @GetMapping
    public List<Order> getAllOrders() {
        return orderService.getAllOrders();
    }

    @GetMapping("/outbox/pending")
    public List<PendingOutboxMessage> getPendingOutbox() {
        return outboxRepository.findAll(Sort.by("id")).stream()
                .map(m -> new PendingOutboxMessage(m.getId(), m.getTopic(), m.getMessageKey(),
                        m.getPayloadType(), m.getCreatedAt()))
                .toList();
    }
}
