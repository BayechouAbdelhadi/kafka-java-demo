package com.example.kafka.common.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Blocking retries then dead-letter to {@code <topic>-dlt} (same partition) for every @KafkaListener.
 * Spring Boot wires this CommonErrorHandler into the default listener container factory.
 * Listeners using @RetryableTopic get their own error handling instead.
 * <p>
 * Once the DLT acknowledged the record, its offset is committed and the partition moves on. If the DLT
 * publication fails, the offset is not committed and the record is retried: it is never dropped.
 * Poison pills are forwarded as their original bytes (see {@link AvroOrBytesSerializer}).
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 2));
    }
}
