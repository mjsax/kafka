/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * {@link MetadataTopicManager} manages an application's metadata topic that is used to store "stop offsets" for batch mode.
 */
class MetadataTopicManager {
    final static String METADATA_TOPIC_NAME_SUFFIX = "-metadata";

    private final Consumer<byte[], byte[]> consumer;
    private final Producer<byte[], byte[]> producer;

    private final StreamsKafkaClient streamsKafkaClient;
    private final String applicationId;
    private final String metadataTopicName;
    private final TopicPartition metadataPartition;
    private final Set<TopicPartition> metadataPartitions;

    private final StringSerializer stringSerializer = new StringSerializer();
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final LongSerializer longSerializer = new LongSerializer();
    private final LongDeserializer longDeserializer = new LongDeserializer();

    MetadataTopicManager(final StreamsConfig streamsConfig,
                         Consumer<byte[], byte[]> consumer,
                         Producer<byte[], byte[]> producer) {
        this.streamsKafkaClient = new StreamsKafkaClient(streamsConfig);
        this.applicationId = streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.metadataTopicName = applicationId + METADATA_TOPIC_NAME_SUFFIX;
        this.consumer = consumer;
        this.producer = producer;

        metadataPartition = new TopicPartition(metadataTopicName, 0);
        metadataPartitions = Collections.singleton(metadataPartition);
    }

    void maybeWriteNewStopOffsets(final Collection<TopicPartition> partitions) {
        final Map<TopicPartition, Long> stopOffsets = getStopOffsetsFromMetadataTopic();

        if (stopOffsets == null) {
            getNewStopOffsets(partitions);
        }
    }

    Map<TopicPartition, Long> getStopOffsetsFromMetadataTopic() {
        if (!metadataTopicExist()) {
            return null;

        }
        final Set<TopicPartition> originalAssignment = consumer.assignment();

        try {
            consumer.assign(metadataPartitions);
            consumer.seekToBeginning(metadataPartitions);

            final long endOfPartition = consumer.endOffsets(metadataPartitions).get(metadataPartition);

            final Map<TopicPartition, Long> stopOffsets = new HashMap<>();

            boolean foundValidOffsets = false;
            loop: while (true) {
                for (final ConsumerRecord<byte[], byte[]> record : consumer.poll(0)) {
                    final String key = stringDeserializer.deserialize(null, record.key());
                    if (isMarker(key)) {
                        if (record.value() != null) {
                            foundValidOffsets = true;
                        } else {
                            foundValidOffsets = false;
                        }
                    } else {
                        final String[] topicAndPartition = key.split("-");
                        final TopicPartition partition = new TopicPartition(
                            topicAndPartition[0],
                            Integer.parseInt(topicAndPartition[1]));
                        final Long offset = longDeserializer.deserialize(null, record.value());

                        if (offset == null) {
                            foundValidOffsets = false;
                        }
                        stopOffsets.put(partition, offset);
                    }

                    if (record.offset() == endOfPartition) {
                        break loop;
                    }
                }
            }

            if (foundValidOffsets) {
                return stopOffsets;
            }
        } finally {
            consumer.assign(originalAssignment);
        }

        return null;
    }

    void cleanMetadataTopic() {
        if (!metadataTopicExist()) {
            return;
        }

        final Set<TopicPartition> originalAssignment = consumer.assignment();
        try {
            consumer.assign(metadataPartitions);
            consumer.seekToBeginning(metadataPartitions);

            final long endOfPartition = consumer.endOffsets(metadataPartitions).get(metadataPartition);

            // get all "active" records, ie, all records that do not have a later tombstone
            final Map<String, byte[]> keys = new HashMap<>();
            loop: while (true) {
                for (final ConsumerRecord<byte[], byte[]> record : consumer.poll(0)) {
                    final String key = stringDeserializer.deserialize(null, record.key());
                    if (record.value() != null) {
                        keys.put(key, record.key());
                    } else {
                        keys.remove(key);
                    }

                    if (record.offset() == endOfPartition) {
                        break loop;
                    }
                }
            }

            // write tombstone for all "active" records
            for (Map.Entry<String, byte[]> key : keys.entrySet()) {
                producer.send(new ProducerRecord<byte[], byte[]>(metadataTopicName, 0, key.getValue(), null));
            }
            producer.flush();
        } finally {
            consumer.assign(originalAssignment);
        }
    }

    private Map<TopicPartition,Long> getNewStopOffsets(final Collection<TopicPartition> partitions) {
        return null;
    }

    // protected to simplify make testing
    protected boolean metadataTopicExist() {
        Collection<MetadataResponse.TopicMetadata> meta = streamsKafkaClient.fetchTopicMetadata();
        for (MetadataResponse.TopicMetadata m : meta) {
            if (m.topic().equals(metadataTopicName)) {
                return true;
            }
        }
        return false;
    }

    private void createMetadataTopic() {
        final InternalTopicConfig metadataTopicConfig = new InternalTopicConfig(
            metadataTopicName,
            Collections.singleton(InternalTopicConfig.CleanupPolicy.compact),
            new HashMap<String, String>()
        );
        final Map<InternalTopicConfig, Integer> metadataTopicSpecification = new HashMap<>();
        metadataTopicSpecification.put(metadataTopicConfig, 1);

        streamsKafkaClient.createTopics(metadataTopicSpecification, 1, -1);
    }

    private boolean isMarker(final String key) {
        return key.equals(applicationId);
    }


}
