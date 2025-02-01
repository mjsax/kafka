/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.ConsumedInternal;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Collections;
import java.util.Iterator;

/**
 * Used to represent either a KTable source or a GlobalKTable source. A boolean flag is used to indicate if this represents a GlobalKTable a {@link
 * org.apache.kafka.streams.kstream.GlobalKTable}
 */
public class TableSourceNode<KIn, V> extends SourceGraphNode<KIn, V> {

    private boolean shouldReuseSourceTopicForChangelog = false;

    private final ProcessorParameters<KIn, V, KIn, Change<V>> processorParameters;
    protected final String sourceName;

    protected TableSourceNode(final String nodeName,
                              final String sourceName,
                              final String topic,
                              final ConsumedInternal<KIn, V> consumedInternal,
                              final ProcessorParameters<KIn, V, KIn, Change<V>> processorParameters) {
        super(nodeName,
              Collections.singletonList(topic),
              consumedInternal);

        this.sourceName = sourceName;
        this.processorParameters = processorParameters;
    }


    public void reuseSourceTopicForChangeLog(final boolean shouldReuseSourceTopicForChangelog) {
        this.shouldReuseSourceTopicForChangelog = shouldReuseSourceTopicForChangelog;
    }

    @Override
    public String toString() {
        return "TableSourceNode{" +
               ", processorParameters=" + processorParameters +
               ", sourceName='" + sourceName + '\'' +
               "} " + super.toString();
    }

    public static <K, V> TableSourceNodeBuilder<K, V> tableSourceNodeBuilder() {
        return new TableSourceNodeBuilder<>();
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

        topologyBuilder.addSource(consumedInternal().offsetResetPolicy(),
                                  sourceName,
                                  consumedInternal().timestampExtractor(),
                                  consumedInternal().keyDeserializer(),
                                  consumedInternal().valueDeserializer(),
                                  topicName);

        processorParameters.addProcessorTo(topologyBuilder, new String[] {sourceName});

        // if the KTableSource should not be materialized, stores will be null or empty
        final KTableSource<KIn, V> tableSource = processorParameters.processorSupplier();
        if (tableSource.stores() != null) {
            if (shouldReuseSourceTopicForChangelog) {
                tableSource.stores().forEach(store -> {
                    store.withLoggingDisabled();
                    topologyBuilder.connectSourceStoreAndTopic(store.name(), topicName);
                });
            }
        }
    }

    public static class TableSourceNodeBuilder<K, V> {

        protected String nodeName;
        protected String sourceName;
        protected String topic;
        protected ConsumedInternal<K, V> consumedInternal;
        private ProcessorParameters<K, V, K, Change<V>> processorParameters;

        protected TableSourceNodeBuilder() { }

        public TableSourceNodeBuilder<K, V> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withConsumedInternal(final ConsumedInternal<K, V> consumedInternal) {
            this.consumedInternal = consumedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withProcessorParameters(final ProcessorParameters<K, V, K, Change<V>> processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNode<K, V> build() {
            return new TableSourceNode<>(
                nodeName,
                sourceName,
                topic,
                consumedInternal,
                processorParameters
            );
        }
    }
}
