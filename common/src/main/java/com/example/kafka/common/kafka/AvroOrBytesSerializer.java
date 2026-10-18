package com.example.kafka.common.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Value serializer of every producer: Avro for records, raw bytes passed through untouched.
 * Raw bytes only come from poison pills forwarded to a DLT (the payload that failed deserialization).
 * Avro-serializing them would register a "bytes" schema on the DLT subject, after which every Avro
 * record sent to that DLT is rejected as incompatible and the failing partition blocks forever.
 */
public class AvroOrBytesSerializer implements Serializer<Object> {

    private final KafkaAvroSerializer avro = new KafkaAvroSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        avro.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return data instanceof byte[] raw ? raw : avro.serialize(topic, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        return data instanceof byte[] raw ? raw : avro.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        avro.close();
    }
}
