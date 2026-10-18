package com.example.kafka.order;

import com.example.kafka.avro.InventoryResultEvent;
import com.example.kafka.avro.OrderEvent;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.repository.OutboxMessageRepository;
import com.example.kafka.order.dto.CreateOrderRequest;
import com.example.kafka.order.entity.Order;
import com.example.kafka.order.service.OrderService;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://it",
        "app.database=order_test",
        "spring.flyway.clean-disabled=false",
        "app.outbox.poll-interval-ms=100"
})
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.ORDERS_EVENTS, KafkaTopics.INVENTORY_EVENTS, "inventory.events-dlt"},
        brokerProperties = "auto.create.topics.enable=false")
class OrderFlowTest {

    /** Starts every run from an empty schema: the test database persists between runs. */
    @TestConfiguration
    static class CleanDatabase {
        @Bean
        FlywayMigrationStrategy cleanThenMigrate() {
            return flyway -> {
                flyway.clean();
                flyway.migrate();
            };
        }
    }

    @Autowired OrderService orderService;
    @Autowired OutboxMessageRepository outbox;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired EmbeddedKafkaBroker broker;

    @Test
    void orderEventIsRelayedFromOutboxAndInventoryResultConfirmsOrder() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("it-orders", "true", broker);
        props.put("schema.registry.url", "mock://it");
        props.put("specific.avro.reader", true);
        try (Consumer<String, Object> orders = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new KafkaAvroDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(orders, KafkaTopics.ORDERS_EVENTS);

            Order order = orderService.createOrder(new CreateOrderRequest("alice",
                    List.of(new CreateOrderRequest.ItemRequest("P1", 2, 10.0))));

            var record = KafkaTestUtils.getSingleRecord(orders, KafkaTopics.ORDERS_EVENTS, Duration.ofSeconds(20));
            assertThat(record.key()).isEqualTo(order.getId());
            assertThat(((OrderEvent) record.value()).getTotalAmount()).isEqualTo(20.0);
            await().atMost(Duration.ofSeconds(5)).until(() -> outbox.count() == 0);

            kafkaTemplate.send(KafkaTopics.INVENTORY_EVENTS, order.getId(), InventoryResultEvent.newBuilder()
                    .setEventId(UUID.randomUUID().toString())
                    .setEventType("INVENTORY_RESERVED")
                    .setOccurredAt(System.currentTimeMillis())
                    .setAggregateId(order.getId())
                    .setOrderId(order.getId())
                    .setSuccess(true)
                    .build());

            await().atMost(Duration.ofSeconds(20))
                    .until(() -> orderService.getOrder(order.getId()).getStatus() == Order.OrderStatus.CONFIRMED);
        }
    }
}
