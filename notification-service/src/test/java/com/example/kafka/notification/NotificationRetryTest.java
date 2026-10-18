package com.example.kafka.notification;

import com.example.kafka.avro.OrderEvent;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.repository.ProcessedEventRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
        "spring.datasource.url=jdbc:h2:mem:notification;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password="
})
// Same topic names as kafka-init in docker-compose.yml; auto-creation is off so a naming mismatch fails.
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.ORDERS_EVENTS, "orders.events-dlt",
        "orders.events-notification-retry", "orders.events-notification-dlt"},
        brokerProperties = "auto.create.topics.enable=false")
class NotificationRetryTest {

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
        Map<String, Object> props = KafkaTestUtils.consumerProps("it-dlt", "true", broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", "mock://it");
        props.put("specific.avro.reader", true);
        try (Consumer<String, Object> dlt = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new KafkaAvroDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(dlt, "orders.events-notification-dlt");

            OrderEvent event = order("invalid_eve");
            kafkaTemplate.send(KafkaTopics.ORDERS_EVENTS, event.getOrderId(), event);

            var record = KafkaTestUtils.getSingleRecord(dlt, "orders.events-notification-dlt", Duration.ofSeconds(20));
            assertThat(((OrderEvent) record.value()).getEventId()).isEqualTo(event.getEventId());
            assertThat(processedEvents.existsById(event.getEventId())).isFalse();
        }
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
