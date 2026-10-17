package com.example.kafka.order.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public class CreateOrderRequest {

    @NotBlank(message = "customerId est obligatoire")
    private String customerId;

    @NotEmpty(message = "La commande doit contenir au moins un article")
    private List<ItemRequest> items;

    public static class ItemRequest {
        @NotBlank
        private String productId;
        private int quantity;
        private double unitPrice;

        public ItemRequest() {}

        public ItemRequest(String productId, int quantity, double unitPrice) {
            this.productId = productId;
            this.quantity = quantity;
            this.unitPrice = unitPrice;
        }

        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public double getUnitPrice() { return unitPrice; }
        public void setUnitPrice(double unitPrice) { this.unitPrice = unitPrice; }
    }

    public CreateOrderRequest() {}

    public CreateOrderRequest(String customerId, List<ItemRequest> items) {
        this.customerId = customerId;
        this.items = items;
    }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public List<ItemRequest> getItems() { return items; }
    public void setItems(List<ItemRequest> items) { this.items = items; }
}
