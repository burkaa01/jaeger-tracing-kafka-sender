package com.github.burkaa01.tracing.tracing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

public class TracingConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TracingConsumer.class);
    private static final int SLEEP_SECONDS = 20;

    private final JaegerHttpSender jaegerHttpSender;

    public TracingConsumer(String jaegerHost, Integer jaegerPort) {
        jaegerHttpSender = new JaegerHttpSender(jaegerHost, jaegerPort);
    }

    @KafkaListener(topics = "${topics.source-topic}", containerFactory = "tracingContainerFactory")
    public void listen(ConsumerRecord<String, byte[]> record, Acknowledgment acks) {

        /* send message to jaeger */
        send(record);

        /* acknowledge message */
        acks.acknowledge();
    }

    private void send(ConsumerRecord<String, byte[]> record) {
        if (record == null || record.value() == null) {
            return;
        }
        try {
            jaegerHttpSender.send(record.value());
        } catch (Exception exception) {
            String message = String.format("could not send spans to jaeger, retrying offset %s", record.offset());
            LOGGER.error("{}, but first sleeping for {} seconds", message, SLEEP_SECONDS);
            sleep(SLEEP_SECONDS);
            throw new IllegalStateException(message, exception);
        }
    }

    private static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException exception) {
            LOGGER.trace("could not sleep", exception);
        }
    }
}
