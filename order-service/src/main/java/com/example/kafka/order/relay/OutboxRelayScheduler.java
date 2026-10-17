package com.example.kafka.order.relay;

import com.example.kafka.avro.OrderEvent;
import com.example.kafka.common.entity.OutboxMessage;
import com.example.kafka.common.repository.OutboxMessageRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
public class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxMessageRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxRelayScheduler(OutboxMessageRepository outboxRepository,
                                KafkaTemplate<String, Object> kafkaTemplate,
                                ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Polling périodique des messages Outbox en statut PENDING.
     * Publie sur Kafka avec la clé de partitionnement (orderId).
     */
    @Scheduled(fixedDelay = 1500)
    @Transactional
    public void processPendingOutboxMessages() {
        List<OutboxMessage> pending = outboxRepository.findPendingMessages(PageRequest.of(0, 20));
        if (pending.isEmpty()) {
            return;
        }

        log.debug("Outbox Relay : {} message(s) en attente de publication", pending.size());

        for (OutboxMessage msg : pending) {
            try {
                // Désérialisation du payload JSON vers l'objet Avro correspondant
                OrderEvent event = objectMapper.readValue(msg.getPayloadJson(), OrderEvent.class);

                // Publication asynchrone sur Kafka avec rappel de confirmation
                kafkaTemplate.send(msg.getTopic(), msg.getPartitionKey(), event)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                msg.setStatus(OutboxMessage.Status.PUBLISHED);
                                msg.setPublishedAt(Instant.now());
                                outboxRepository.save(msg);
                                log.info("Outbox Relay : message {} publié avec succès sur '{}' (partition: {}, offset: {})",
                                        msg.getId(),
                                        msg.getTopic(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            } else {
                                msg.setRetryCount(msg.getRetryCount() + 1);
                                msg.setLastError(ex.getMessage());
                                outboxRepository.save(msg);
                                log.error("Outbox Relay : échec de publication pour le message {}", msg.getId(), ex);
                            }
                        });
            } catch (Exception e) {
                log.error("Outbox Relay : erreur de traitement du message {}", msg.getId(), e);
                msg.setRetryCount(msg.getRetryCount() + 1);
                msg.setLastError(e.getMessage());
                outboxRepository.save(msg);
            }
        }
    }
}
