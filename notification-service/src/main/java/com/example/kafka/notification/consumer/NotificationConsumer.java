package com.example.kafka.notification.consumer;

import com.example.kafka.avro.OrderEvent;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.common.entity.ProcessedEvent;
import com.example.kafka.common.repository.ProcessedEventRepository;
import com.example.kafka.notification.exception.PermanentNotificationException;
import com.example.kafka.notification.exception.TransientNotificationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final ProcessedEventRepository processedEvents;
    // Simulates a flaky provider: fails the first 2 attempts of a "flaky_" customer
    private final Map<String, Integer> flakyAttempts = new ConcurrentHashMap<>();

    public NotificationConsumer(ProcessedEventRepository processedEvents) {
        this.processedEvents = processedEvents;
    }

    /**
     * Non-blocking retries: a failed record moves to orders.events-notification-retry (every 2s, up to
     * 4 attempts in total) then to orders.events-notification-dlt, without blocking the main partition.
     * PermanentNotificationException goes straight to the DLT. Topics are created by kafka-init.
     */
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            retryTopicSuffix = "-notification-retry",
            dltTopicSuffix = "-notification-dlt",
            autoCreateTopics = "false",
            exclude = PermanentNotificationException.class)
    @KafkaListener(topics = KafkaTopics.ORDERS_EVENTS)
    @Transactional
    public void onOrderEvent(OrderEvent event, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        if (processedEvents.existsById(event.getEventId())) {
            log.info("Duplicate event {}, notification already sent", event.getEventId());
            return;
        }

        send(event);
        processedEvents.save(new ProcessedEvent(event.getEventId()));
        log.info("Notified customer {} for order {} (from '{}')", event.getCustomerId(), event.getOrderId(), topic);
    }

    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, OrderEvent> record,
                             @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String error) {
        log.error("Notification dead-lettered: order={} partition={} offset={} error={}",
                record.key(), record.partition(), record.offset(), error);
    }

    private void send(OrderEvent event) {
        String customerId = event.getCustomerId();
        if (customerId.startsWith("invalid_")) {
            throw new PermanentNotificationException("Invalid customer: " + customerId);
        }
        if (customerId.startsWith("flaky_")) {
            int attempt = flakyAttempts.merge(event.getEventId(), 1, Integer::sum);
            if (attempt < 3) {
                throw new TransientNotificationException("Provider unavailable (attempt " + attempt + ")");
            }
            flakyAttempts.remove(event.getEventId());
        }
        // Real channel (email/SMS) call goes here.
    }
}
