package com.example.kafka.inventory.controller;

import com.example.kafka.inventory.entity.Stock;
import com.example.kafka.inventory.repository.StockRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/inventory")
public class InventoryController {

    private final StockRepository stockRepository;

    public InventoryController(StockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @GetMapping
    public ResponseEntity<List<Stock>> getAllStock() {
        return ResponseEntity.ok(stockRepository.findAll());
    }

    @GetMapping("/{productId}")
    public ResponseEntity<Stock> getStock(@PathVariable String productId) {
        return stockRepository.findById(productId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{productId}/init")
    public ResponseEntity<Stock> initStock(@PathVariable String productId,
                                           @RequestParam(defaultValue = "100") int quantity) {
        Stock stock = stockRepository.findById(productId)
                .orElse(new Stock(productId, quantity));
        stock.setAvailableQuantity(quantity);
        return ResponseEntity.ok(stockRepository.save(stock));
    }
}
