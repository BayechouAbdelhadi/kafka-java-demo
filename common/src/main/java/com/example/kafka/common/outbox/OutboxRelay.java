package com.example.kafka.common.outbox;

import com.example.kafka.common.entity.OutboxMessage;
import com.example.kafka.common.repository.OutboxMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Polls the outbox, publishes to Kafka and deletes what the brokers acknowledged — all inside one
 * DB transaction holding row locks, so a row is either published-and-deleted or retried (at-least-once).
 * Enabled with {@code app.outbox.enabled=true}.
 */
@Component
@EnableScheduling
@ConditionalOnProperty(name = "app.outbox.enabled", havingValue = "true")
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;
    private static final long SEND_TIMEOUT_SECONDS = 30;

    private final OutboxMessageRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxRelay(OutboxMessageRepository repository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:500}")
    @Transactional
    public void publishPending() {
        List<OutboxMessage> batch = repository.lockNextBatch(BATCH_SIZE);
        if (batch.isEmpty()) {
            return;
        }

        // Send the whole batch first so the producer can pipeline it, then wait for the acks.
        // The idempotent producer keeps per-partition order, so per-key order is preserved.
        List<CompletableFuture<SendResult<String, Object>>> sends = batch.stream()
                .map(m -> kafkaTemplate.send(m.getTopic(), m.getMessageKey(),
                        AvroCodec.decode(m.getPayloadType(), m.getPayload())))
                .toList();

        List<OutboxMessage> published = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            OutboxMessage message = batch.get(i);
            try {
                sends.get(i).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                published.add(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException | TimeoutException e) {
                log.warn("Outbox message {} to '{}' not published, will retry: {}",
                        message.getId(), message.getTopic(), e.getMessage());
            }
        }

        repository.deleteAllInBatch(published);
        log.debug("Outbox relay published {}/{} message(s)", published.size(), batch.size());
    }
}
