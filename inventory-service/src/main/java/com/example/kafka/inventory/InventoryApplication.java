package com.example.kafka.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = {"com.example.kafka.inventory.entity", "com.example.kafka.common.entity"})
@EnableJpaRepositories(basePackages = {"com.example.kafka.inventory.repository", "com.example.kafka.common.repository"})
public class InventoryApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryApplication.class, args);
    }
}
