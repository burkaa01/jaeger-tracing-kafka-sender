package com.github.burkaa01.consumer.config.tracing;

import io.jaegertracing.spi.Sender;

public class KafkaSenderConfiguration extends io.jaegertracing.Configuration.SenderConfiguration {

    private final String bootstrapServers;
    private final String tracingTopic;

    public KafkaSenderConfiguration(String bootstrapServers, String tracingTopic) {
        super();
        this.bootstrapServers = bootstrapServers;
        this.tracingTopic = tracingTopic;
    }

    @Override
    public Sender getSender() {
        return new KafkaSender(bootstrapServers, tracingTopic);
    }
}
