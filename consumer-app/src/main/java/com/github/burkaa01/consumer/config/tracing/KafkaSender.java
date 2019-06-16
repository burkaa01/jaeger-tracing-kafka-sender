package com.github.burkaa01.consumer.config.tracing;

import io.jaegertracing.internal.exceptions.SenderException;
import io.jaegertracing.thrift.internal.senders.ThriftSender;
import io.jaegertracing.thriftjava.Batch;
import io.jaegertracing.thriftjava.Process;
import io.jaegertracing.thriftjava.Span;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaSender extends ThriftSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);
    private static final int ONE_MB_IN_BYTES = 1048576;

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;

    public KafkaSender(String bootstrapServers, String tracingTopic) {
        super(ProtocolType.Binary, ONE_MB_IN_BYTES);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(properties);
        topic = tracingTopic;
    }

    @Override
    public void send(Process process, List<Span> spans) throws SenderException {
        Batch batch = new Batch(process, spans);
        byte[] bytes;
        try {
            bytes = serialize(batch);
        } catch (Exception e) {
            throw new SenderException(String.format("Failed to serialize %d spans", spans.size()), e, spans.size());
        }
        if (bytes != null) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, bytes);
            producer.send(record, (RecordMetadata recordMetadata, Exception exception) -> {
                if (exception != null) {
                    LOGGER.error(String.format("Could not send %d spans", spans.size()), exception);
                }
            });
        }
    }

    @Override
    public int close() throws SenderException {
        try {
            return super.close();
        } finally {
            producer.close();
        }
    }
}
