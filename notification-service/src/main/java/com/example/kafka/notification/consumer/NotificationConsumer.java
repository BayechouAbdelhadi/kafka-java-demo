package com.example.kafka.notification.consumer;

import com.example.kafka.avro.OrderEvent;
import com.example.kafka.common.constant.KafkaTopics;
import com.example.kafka.notification.exception.PermanentNotificationException;
import com.example.kafka.notification.exception.TransientNotificationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    // Compteur de simulation pour provoquer des erreurs transitoires reproductibles en démo
    private final ConcurrentHashMap<String, AtomicInteger> attemptCounters = new ConcurrentHashMap<>();

    /**
     * Consommateur avec stratégie de non-blocking retries et DLT :
     * - 3 tentatives au total (1 initial + 2 retries).
     * - Backoff exponentiel (1s puis 2s).
     * - Erreur transitoire (TransientNotificationException) -> Retentée.
     * - Erreur permanente (PermanentNotificationException) -> Déroutée immédiatement vers la DLT.
     */
    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR,
            include = {TransientNotificationException.class},
            exclude = {PermanentNotificationException.class}
    )
    @KafkaListener(topics = KafkaTopics.ORDERS_EVENTS, groupId = "notification-service-group")
    public void onOrderEvent(ConsumerRecord<String, OrderEvent> record,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        OrderEvent event = record.value();
        String customerId = event.getCustomerId();

        log.info("🔔 [Notification] Traitement événement commande {} sur le topic '{}' (client: {})",
                event.getOrderId(), topic, customerId);

        // Scénario de Démonstration 1 : Erreur Permanente (ex: client invalide commençant par 'invalid_')
        if (customerId != null && customerId.startsWith("invalid_")) {
            log.error("❌ [ERREUR PERMANENTE] Client invalide identifié : {}. Envoi direct en DLT sans retry inutile.", customerId);
            throw new PermanentNotificationException("Format client corrompu ou invalide: " + customerId);
        }

        // Scénario de Démonstration 2 : Erreur Transitoire (ex: client commençant par 'flaky_')
        // Échoue 2 fois puis réussit à la 3e tentative !
        if (customerId != null && customerId.startsWith("flaky_")) {
            AtomicInteger counter = attemptCounters.computeIfAbsent(event.getOrderId(), k -> new AtomicInteger(0));
            int currentAttempt = counter.incrementAndGet();
            if (currentAttempt < 3) {
                log.warn("⚠️ [ERREUR TRANSITOIRE] Échec réseau simulé vers le fournisseur SMS (tentative {}/3)...", currentAttempt);
                throw new TransientNotificationException("Serveur SMS tiers temporairement indisponible (tentative " + currentAttempt + ")");
            }
            log.info("🎉 [RÉTABLISSEMENT] Succès de la notification SMS à la tentative {}", currentAttempt);
            attemptCounters.remove(event.getOrderId());
            return;
        }

        // Traitement nominal
        log.info("✅ [NOTIFIÉ] Notification de confirmation envoyée au client {} pour la commande {}.",
                customerId, event.getOrderId());
    }

    /**
     * Handler DLT exécuté lorsqu'un message a épuisé tous ses retries ou a subi une erreur permanente.
     */
    @DltHandler
    public void handleDeadLetterTopic(ConsumerRecord<String, OrderEvent> record,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        OrderEvent event = record.value();
        log.error("💀 [DLT] Message échoué stocké dans la Dead Letter Topic '{}' | Commande: {} | Offset: {}",
                topic, event.getOrderId(), record.offset());
        // En production : enregistrement dans une table d'investigation ou alerte Slack/PagerDuty
    }
}
