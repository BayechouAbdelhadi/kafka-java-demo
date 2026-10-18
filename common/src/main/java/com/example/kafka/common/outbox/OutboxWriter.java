package com.example.kafka.common.outbox;

import com.example.kafka.common.entity.OutboxMessage;
import com.example.kafka.common.repository.OutboxMessageRepository;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxWriter {

    private final OutboxMessageRepository repository;

    public OutboxWriter(OutboxMessageRepository repository) {
        this.repository = repository;
    }

    /** Must join the caller's transaction so the event is committed atomically with the business change. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void enqueue(String topic, String key, SpecificRecord event) {
        repository.save(new OutboxMessage(topic, key, event.getClass().getName(), AvroCodec.encode(event)));
    }
}
