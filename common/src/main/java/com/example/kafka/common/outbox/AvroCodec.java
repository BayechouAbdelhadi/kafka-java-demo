package com.example.kafka.common.outbox;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.util.ClassUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/** Avro binary encoding of generated records, used to store events in the outbox table. */
final class AvroCodec {

    private AvroCodec() {}

    static byte[] encode(SpecificRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new SpecificDatumWriter<SpecificRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static SpecificRecord decode(String type, byte[] payload) {
        try {
            Class<?> recordClass = ClassUtils.forName(type, AvroCodec.class.getClassLoader());
            Schema schema = SpecificData.get().getSchema(recordClass);
            return new SpecificDatumReader<SpecificRecord>(schema)
                    .read(null, DecoderFactory.get().binaryDecoder(payload, null));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unknown outbox payload type " + type, e);
        }
    }
}
