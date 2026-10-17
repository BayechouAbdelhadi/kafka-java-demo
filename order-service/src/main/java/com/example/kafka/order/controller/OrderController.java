package com.example.kafka.order.controller;

import com.example.kafka.common.entity.OutboxMessage;
import com.example.kafka.common.repository.OutboxMessageRepository;
import com.example.kafka.order.dto.CreateOrderRequest;
import com.example.kafka.order.entity.Order;
import com.example.kafka.order.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;
    private final OutboxMessageRepository outboxRepository;

    public OrderController(OrderService orderService, OutboxMessageRepository outboxRepository) {
        this.orderService = orderService;
        this.outboxRepository = outboxRepository;
    }

    @PostMapping
    public ResponseEntity<Order> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        Order created = orderService.createOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> getOrder(@PathVariable String id) {
        return ResponseEntity.ok(orderService.getOrder(id));
    }

    @GetMapping
    public ResponseEntity<List<Order>> getAllOrders() {
        return ResponseEntity.ok(orderService.getAllOrders());
    }

    @GetMapping("/outbox/pending")
    public ResponseEntity<List<OutboxMessage>> getPendingOutbox() {
        return ResponseEntity.ok(outboxRepository.findPendingMessages(PageRequest.of(0, 100)));
    }
}
