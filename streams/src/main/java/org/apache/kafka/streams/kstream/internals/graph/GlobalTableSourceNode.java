package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Iterator;

public class GlobalTableSourceNode<K, V> extends TableSourceNode<K, V> {
    protected final ProcessorParameters<K, V, Void, Void> processorParameters;

    private GlobalTableSourceNode(final String nodeName,
                                  final String sourceName,
                                  final String topic,
                                  final ConsumedInternal<K, V> consumedInternal,
                                  final ProcessorParameters<K, V, Void, Void> processorParameters) {
        super(nodeName, sourceName, topic, consumedInternal, null);

        this.processorParameters = processorParameters;
    }

    @Override
    public String toString() {
        return "GloabelTableSourceNode{" +
            ", processorParameters=" + processorParameters +
            ", sourceName='" + sourceName + '\'' +
            "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String topicName;
        if (topicNames().isPresent()) {
            final Iterator<String> topicNames = topicNames().get().iterator();
            topicName = topicNames.next();
            if (topicNames.hasNext()) {
                throw new IllegalStateException("A table source node must have a single topic as input");
            }
        } else {
            throw new IllegalStateException("A table source node must have a single topic as input");
        }

        topologyBuilder.addGlobalStore(
            sourceName,
            consumedInternal().timestampExtractor(),
            consumedInternal().keyDeserializer(),
            consumedInternal().valueDeserializer(),
            topicName,
            processorParameters.processorName(),
            (ProcessorSupplier<K, V, Void, Void>) processorParameters.processorSupplier(),
            false
        );
    }

    public static <K, V> GlobalTableSourceNodeBuilder<K, V> globalTableSourceNodeBuilder() {
        return new GlobalTableSourceNodeBuilder<>();
    }

    public static class GlobalTableSourceNodeBuilder<K, V> extends TableSourceNodeBuilder<K, V> {
        protected ProcessorParameters<K, V, Void, Void> processorParameters;

        @Override
        public GlobalTableSourceNodeBuilder<K, V> withSourceName(final String sourceName) {
            super.withSourceName(sourceName);
            return this;
        }

        @Override
        public GlobalTableSourceNodeBuilder<K, V> withTopic(final String topic) {
            super.withTopic(topic);
            return this;
        }

        @Override
        public GlobalTableSourceNodeBuilder<K, V> withConsumedInternal(final ConsumedInternal<K, V> consumedInternal) {
            super.withConsumedInternal(consumedInternal);
            return this;
        }

        @Override
        public GlobalTableSourceNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters<K, V, K, Change<V>> processorParameters) {
            throw new UnsupportedOperationException("Use withGlobalProcessorParameters() instead");
        }

        public GlobalTableSourceNodeBuilder<K, V> withGlobalProcessorParameters(final ProcessorParameters<K, V, Void, Void> processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        @Override
        public GlobalTableSourceNodeBuilder<K, V> withNodeName(final String nodeName) {
            super.withNodeName(nodeName);
            return this;
        }

        public GlobalTableSourceNode<K, V> build() {
            return new GlobalTableSourceNode<>(
                nodeName,
                sourceName,
                topic,
                consumedInternal,
                processorParameters
            );
        }
    }
}