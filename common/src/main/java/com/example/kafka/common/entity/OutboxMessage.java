package com.example.kafka.common.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Transactional outbox row. Written in the same DB transaction as the business change,
 * published then deleted by {@link com.example.kafka.common.outbox.OutboxRelay}.
 * The payload is the Avro binary encoding of the event; {@code payloadType} is its class name.
 */
@Entity
@Table(name = "outbox_messages")
public class OutboxMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "topic", nullable = false, length = 100)
    private String topic;

    @Column(name = "message_key", nullable = false, length = 64)
    private String messageKey;

    @Column(name = "payload_type", nullable = false, length = 200)
    private String payloadType;

    @Column(name = "payload", nullable = false)
    private byte[] payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected OutboxMessage() {}

    public OutboxMessage(String topic, String messageKey, String payloadType, byte[] payload) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.payloadType = payloadType;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    public Long getId() { return id; }
    public String getTopic() { return topic; }
    public String getMessageKey() { return messageKey; }
    public String getPayloadType() { return payloadType; }
    public byte[] getPayload() { return payload; }
    public Instant getCreatedAt() { return createdAt; }
}
