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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class MetadataTopicManagerTest {
    private static final String applicationId = "test-app";
    private static final Properties configParameters = new Properties();
    private static StreamsConfig streamsConfig;

    @BeforeClass
    public static void prepareTest() {
        configParameters.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        configParameters.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        streamsConfig = new StreamsConfig(configParameters);
    }

    @Test
    public void getStopOffsets() {
        final MetadataTopicManager topicManager = new MockMetadataTopicManager(streamsConfig, null, null);
    }

    @Test
    public void testCleanup() {
        final StringSerializer stringSerializer = new StringSerializer();
        final StringDeserializer stringDeserializer = new StringDeserializer();

        final String metadataTopicName = applicationId + MetadataTopicManager.METADATA_TOPIC_NAME_SUFFIX;
        final int metadataPartitionNumber = 0;
        final TopicPartition metadataPartition = new TopicPartition(metadataTopicName, metadataPartitionNumber);

        final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        final MockProducer<byte[], byte[]> producer = new MockProducer<>(true, null, null);
        final MetadataTopicManager topicManager = new MockMetadataTopicManager(streamsConfig, consumer, producer);

        final Set<TopicPartition> initialAssignment = new HashSet<>();
        initialAssignment.add(new TopicPartition("someTopic", 3));
        initialAssignment.add(new TopicPartition("someTopic", 9));
        initialAssignment.add(new TopicPartition("someOtherTopic", 4));
        final Map<String, byte[]> result = new HashMap<>();
        final Map<String, byte[]> expectedResult = new HashMap<>();



        // prepare metadata topic
        consumer.assign(Collections.singleton(metadataPartition));

        long offset = 42;
        final Map<TopicPartition, Long> beginOffset= new HashMap<>();
        beginOffset.put(metadataPartition, offset);
        consumer.updateBeginningOffsets(beginOffset);

        consumer.addRecord(new ConsumerRecord<>(metadataTopicName, metadataPartitionNumber, offset++, stringSerializer.serialize(null,"key1"), new byte[]{(byte)0}));
        consumer.addRecord(new ConsumerRecord<>(metadataTopicName, metadataPartitionNumber, offset++, stringSerializer.serialize(null,"key2"), new byte[]{(byte)0}));
        consumer.addRecord(new ConsumerRecord<>(metadataTopicName, metadataPartitionNumber, offset++, stringSerializer.serialize(null,"key3"), new byte[]{(byte)0}));
        consumer.addRecord(new ConsumerRecord<>(metadataTopicName, metadataPartitionNumber, offset++, stringSerializer.serialize(null,"key4"), new byte[]{(byte)0}));
        consumer.addRecord(new ConsumerRecord<>(metadataTopicName, metadataPartitionNumber, offset++, stringSerializer.serialize(null,"key1"), (byte[]) null));
        consumer.addRecord(new ConsumerRecord<>(metadataTopicName, metadataPartitionNumber, offset++, stringSerializer.serialize(null,"key4"), (byte[]) null));
        expectedResult.put("key2" ,null);
        expectedResult.put("key3" ,null);

        final Map<TopicPartition, Long> endOffsets= new HashMap<>();
        endOffsets.put(metadataPartition, offset - 1);
        consumer.updateEndOffsets(endOffsets);

        // run test
        consumer.assign(initialAssignment);
        topicManager.cleanMetadataTopic();

        // verify
        for (final ProducerRecord<byte[], byte[]> record : producer.history()) {
            assertThat(record.topic(), equalTo(metadataTopicName));
            assertThat(record.partition(), equalTo(metadataPartitionNumber));

            result.put(stringDeserializer.deserialize(null, record.key()), record.value());
        }

        assertThat(consumer.assignment(), equalTo(initialAssignment));
        assertThat(result, equalTo(expectedResult));
    }

    private class MockMetadataTopicManager extends MetadataTopicManager {
        public MockMetadataTopicManager(final StreamsConfig streamsConfig, final Consumer consumer, final Producer producer) {
            super (streamsConfig, consumer, producer);
        }

        protected boolean metadataTopicExist() {
            return true;
        }
    };
}
