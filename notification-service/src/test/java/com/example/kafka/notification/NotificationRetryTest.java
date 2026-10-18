package com.example.kafka.notification;

import com.example.kafka.avro.OrderEvent;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.repository.ProcessedEventRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://it",
        "app.database=notification_test",
        "spring.flyway.clean-disabled=false"
})
// Same topic names as kafka-init in docker-compose.yml; auto-creation is off so a naming mismatch fails.
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.ORDERS_EVENTS, "orders.events-dlt",
        "orders.events-notification-retry", "orders.events-notification-dlt"},
        brokerProperties = "auto.create.topics.enable=false")
class NotificationRetryTest {

    private static final String DLT = "orders.events-notification-dlt";

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
    @Autowired ProcessedEventRepository processedEvents;
    @Autowired EmbeddedKafkaBroker broker;

    @Test
    void transientFailureIsRetriedThenSucceeds() {
        OrderEvent event = order("flaky_bob");
        kafkaTemplate.send(KafkaTopics.ORDERS_EVENTS, event.getOrderId(), event);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> processedEvents.existsById(event.getEventId()));
    }

    @Test
    void permanentFailureGoesStraightToDlt() {
        Map<String, Object> props = Map.of("schema.registry.url", "mock://it", "specific.avro.reader", true);
        try (Consumer<String, Object> dlt = dltConsumer("it-dlt", new KafkaAvroDeserializer(), props)) {
            OrderEvent event = order("invalid_eve");
            kafkaTemplate.send(KafkaTopics.ORDERS_EVENTS, event.getOrderId(), event);

            var record = KafkaTestUtils.getSingleRecord(dlt, DLT, Duration.ofSeconds(20));
            assertThat(((OrderEvent) record.value()).getEventId()).isEqualTo(event.getEventId());
            assertThat(processedEvents.existsById(event.getEventId())).isFalse();
        }
    }

    /**
     * Regression: a poison pill dead-lettered first used to register a "bytes" schema on the DLT subject,
     * after which every OrderEvent was rejected by the registry and the partition blocked forever.
     */
    @Test
    void poisonPillThenPermanentFailureBothReachDlt() {
        byte[] pill = "this is not avro".getBytes(StandardCharsets.UTF_8);
        try (Consumer<String, byte[]> dlt = dltConsumer("it-dlt-pill", new ByteArrayDeserializer(), Map.of());
             Producer<String, byte[]> raw = new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker),
                     new StringSerializer(), new ByteArraySerializer()).createProducer()) {
            raw.send(new ProducerRecord<>(KafkaTopics.ORDERS_EVENTS, "poison", pill));
            raw.flush();
            OrderEvent event = order("invalid_frank");
            kafkaTemplate.send(KafkaTopics.ORDERS_EVENTS, event.getOrderId(), event);

            List<ConsumerRecord<String, byte[]>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                KafkaTestUtils.getRecords(dlt, Duration.ofMillis(500)).forEach(received::add);
                return received.size() >= 2;
            });

            assertThat(received.get(0).value()).as("pill forwarded as its original bytes").isEqualTo(pill);
            assertThat(received.get(1).key()).isEqualTo(event.getOrderId());
            assertThat(received.get(1).value()[0]).as("Avro magic byte").isZero();
        }
    }

    /** A consumer of the notification DLT positioned at its current end, so it sees only this test's records. */
    private <V> Consumer<String, V> dltConsumer(String group, Deserializer<V> values, Map<String, Object> extra) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(group, "false", broker);
        props.putAll(extra);
        Consumer<String, V> consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), values)
                .createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, DLT);
        consumer.seekToEnd(consumer.assignment());
        consumer.assignment().forEach(consumer::position); // resolve the end now, before anything is sent
        return consumer;
    }

    private static OrderEvent order(String customerId) {
        String orderId = UUID.randomUUID().toString();
        return OrderEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("ORDER_CREATED")
                .setOccurredAt(System.currentTimeMillis())
                .setAggregateId(orderId)
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setTotalAmount(10)
                .setStatus("CREATED")
                .setItems(List.of())
                .build();
    }
}
