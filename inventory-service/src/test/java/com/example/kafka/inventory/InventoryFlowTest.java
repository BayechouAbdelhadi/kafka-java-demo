package com.example.kafka.inventory;

import com.example.kafka.avro.InventoryResultEvent;
import com.example.kafka.avro.OrderEvent;
import com.example.kafka.avro.OrderItem;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.repository.ProcessedEventRepository;
import com.example.kafka.inventory.entity.Stock;
import com.example.kafka.inventory.repository.StockRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://it",
        "app.database=inventory_test",
        "spring.flyway.clean-disabled=false",
        "app.outbox.poll-interval-ms=100"
})
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.ORDERS_EVENTS, KafkaTopics.INVENTORY_EVENTS,
        "orders.events-dlt", "inventory.events-dlt"},
        brokerProperties = "auto.create.topics.enable=false")
class InventoryFlowTest {

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

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired StockRepository stocks;
    @Autowired ProcessedEventRepository processedEvents;
    @Autowired EmbeddedKafkaBroker broker;

    Consumer<String, Object> results;

    @BeforeEach
    void setUp() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("it-" + UUID.randomUUID(), "true", broker);
        props.put("schema.registry.url", "mock://it");
        props.put("specific.avro.reader", true);
        results = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new KafkaAvroDeserializer())
                .createConsumer();
        broker.consumeFromAnEmbeddedTopic(results, KafkaTopics.INVENTORY_EVENTS);
    }

    @AfterEach
    void tearDown() {
        results.close();
    }

    @Test
    void reservesStockOnceAndPublishesResultThroughOutbox() {
        stocks.save(new Stock("P1", 10));
        OrderEvent event = order("o-1", new OrderItem("P1", 3, 5.0));

        kafkaTemplate.send(KafkaTopics.ORDERS_EVENTS, "o-1", event);
        kafkaTemplate.send(KafkaTopics.ORDERS_EVENTS, "o-1", event); // redelivery

        List<InventoryResultEvent> received = resultsFor("o-1", Duration.ofSeconds(5));
        assertThat(received).as("duplicate must not produce a second result").hasSize(1);
        assertThat(received.get(0).getSuccess()).isTrue();
        assertThat(stocks.findById("P1").orElseThrow().getAvailableQuantity()).isEqualTo(7);
        assertThat(processedEvents.existsById(event.getEventId())).isTrue();
    }

    @Test
    void failedReservationLeavesAllStockUntouched() {
        stocks.save(new Stock("A", 10));
        stocks.save(new Stock("B", 1));

        kafkaTemplate.send(KafkaTopics.ORDERS_EVENTS, "o-2",
                order("o-2", new OrderItem("A", 5, 1.0), new OrderItem("B", 2, 1.0)));

        InventoryResultEvent result = resultsFor("o-2", Duration.ofSeconds(5)).get(0);
        assertThat(result.getSuccess()).isFalse();
        assertThat(result.getReason()).contains("B");
        assertThat(stocks.findById("A").orElseThrow().getAvailableQuantity()).isEqualTo(10);
    }

    /** Collects the results for one order, polling for the given duration. */
    private List<InventoryResultEvent> resultsFor(String orderId, Duration duration) {
        List<InventoryResultEvent> found = new ArrayList<>();
        long deadline = System.currentTimeMillis() + duration.toMillis();
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, Object> record : results.poll(Duration.ofMillis(200))) {
                if (orderId.equals(record.key())) {
                    found.add((InventoryResultEvent) record.value());
                }
            }
        }
        return found;
    }

    private static OrderEvent order(String orderId, OrderItem... items) {
        return OrderEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("ORDER_CREATED")
                .setOccurredAt(System.currentTimeMillis())
                .setAggregateId(orderId)
                .setOrderId(orderId)
                .setCustomerId("c-1")
                .setTotalAmount(0)
                .setStatus("CREATED")
                .setItems(List.of(items))
                .build();
    }
}
