package com.example.kafka.common.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Blocking retries then dead-letter to {@code <topic>-dlt} (same partition) for every @KafkaListener.
 * Spring Boot wires this CommonErrorHandler into the default listener container factory.
 * Listeners using @RetryableTopic get their own error handling instead.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> avroTemplate,
                                                 ProducerFactory<Object, Object> producerFactory) {
        // Records that failed deserialization (poison pills) only have raw bytes: forward them untouched,
        // so they don't register a "bytes" schema on the DLT subject.
        KafkaTemplate<Object, Object> bytesTemplate = new KafkaTemplate<>(producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, avroTemplate);

        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(templates), new FixedBackOff(1000L, 2));
    }
}
