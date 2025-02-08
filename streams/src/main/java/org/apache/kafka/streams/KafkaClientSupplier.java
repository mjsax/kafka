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
package org.apache.kafka.streams;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Map;
import java.util.Properties;

/**
 * {@code KafkaClientSupplier} can be used to provide custom Kafka clients to a {@link KafkaStreams} instance.
 *
 * @see KafkaStreams#KafkaStreams(Topology, java.util.Properties, KafkaClientSupplier)
 */
public interface KafkaClientSupplier {
    /**
     * Create an {@link Admin admin client} which is used for internal topic management.
     *
     * @param config
     *        {@link StreamsConfig#getAdminConfigs(String) admit config} which is supplied by the
     *        {@link java.util.Properties Properties} given to the
     *        {@link KafkaStreams#KafkaStreams(Topology, Properties) KafkaStreams} instance
     *
     * @return An instance of Kafka {@link Admin client}.
     */
    default Admin getAdmin(final Map<String, Object> config) {
        throw new UnsupportedOperationException("Implementations of KafkaClientSupplier should implement the getAdmin() method.");
    }

    /**
     * Create a {@link Producer producer client} which is used to write records to topics.
     *
     * @param config
     *        {@link StreamsConfig#getProducerConfigs(String) producer config} which is supplied by the
     *        {@link java.util.Properties Properties} given to the
     *        {@link KafkaStreams#KafkaStreams(Topology, Properties) KafkaStreams} instance
     *
     * @return An instance of Kafka {@link Producer Producer client}.
     */
    Producer<byte[], byte[]> getProducer(final Map<String, Object> config);

    /**
     * Create a {@link Consumer consumer client} which is used to read records from input and repartition topics.
     * This consumer is called the "main consumer" is forms a consumer group using the
     * {@link StreamsConfig#APPLICATION_ID_CONFIG application.id} as it's {@code group.id}.
     *
     * @param config
     *        {@link StreamsConfig#getMainConsumerConfigs(String, String, int) consumer config} which is supplied by the
     *        {@link java.util.Properties Properties} given to the
     *        {@link KafkaStreams#KafkaStreams(Topology, Properties) KafkaStreams} instance
     *
     * @return An instance of Kafka {@link Consumer Consumer client}.
     */
    Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config);

    /**
     * Create a {@link Consumer consumer client} which is used to read records from changelog topic to restore
     * {@link StateStore}s, and maintain standby tasks.
     * This consumer is called the "restore consumer" and it uses explicit
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer#assign(java.util.Collection) partition assignment},
     * does not form a consumer group, nor does it commit offsets.
     *
     * @param config
     *        {@link StreamsConfig#getRestoreConsumerConfigs(String) restore consumer config} which is supplied
     *        by the {@link java.util.Properties Properties} given to the
     *        {@link KafkaStreams#KafkaStreams(Topology, Properties) KafkaStreams} instance
     *
     * @return An instance of Kafka {@link Consumer Consumer client}.
     */
    Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config);

    /**
     * Create a {@link Consumer consumer client} which is used to consume records for
     * {@link Topology#addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) global state stores}
     * and {@link GlobalKTable}.
     * This consumer is called the "global consumer" and it uses explicit
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer#assign(java.util.Collection) partition assignment},
     * does not form a consumer group, nor does it commit offsets.
     *
     * @param config {@link StreamsConfig#getGlobalConsumerConfigs(String) global consumer config} which is supplied
     *               by the {@link java.util.Properties Properties} given to the
     *               {@link KafkaStreams#KafkaStreams(Topology, Properties) KafkaStreams} instance
     *
     * @return An instance of Kafka {@link Consumer Consumer client}
     */
    Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config);
}