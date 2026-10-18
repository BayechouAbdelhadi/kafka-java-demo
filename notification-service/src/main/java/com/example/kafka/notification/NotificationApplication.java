package com.example.kafka.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/** Scans com.example.kafka so the shared outbox, idempotency and error-handling components are picked up. */
@SpringBootApplication(scanBasePackages = "com.example.kafka")
@EntityScan("com.example.kafka")
@EnableJpaRepositories("com.example.kafka")
public class NotificationApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationApplication.class, args);
    }
}
