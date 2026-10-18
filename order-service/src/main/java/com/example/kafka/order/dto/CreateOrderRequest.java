package com.example.kafka.order.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.List;

public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotEmpty List<@Valid ItemRequest> items) {

    public record ItemRequest(
            @NotBlank String productId,
            @Positive int quantity,
            @PositiveOrZero double unitPrice) {
    }
}
