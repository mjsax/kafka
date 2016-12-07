/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import kafka.admin.AdminClient;
import kafka.utils.MockTime;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

public class IncrementalBatchModeIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static ZkUtils zkUtils = null;
    private static AdminClient adminClient = null;

    public final static String INPUT_TOPIC_1 = "input-1";
    public final static String INPUT_TOPIC_2 = "input-2";
    public final static String OUTPUT_TOPIC = "output";

    private static final long CLEANUP_CONSUMER_TIMEOUT = 2000L;
    private static final int TIMEOUT_MULTIPLYER = 5;

    private final static class TopicsGotDeletedCondition implements TestCondition {
        final String topic;

        TopicsGotDeletedCondition(String topic) {
            this.topic = topic;
        }

        @Override
        public boolean conditionMet() {
            final Set<String> allTopics = new HashSet<>();
            allTopics.addAll(scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics()));
            return !allTopics.contains(topic);
        }
    }

    private class WaitUntilConsumerGroupGotClosed implements TestCondition {
        final String appId;

        WaitUntilConsumerGroupGotClosed(String appId) {
            this.appId = appId;
        }

        @Override
        public boolean conditionMet() {
            return adminClient.describeConsumerGroup(appId).consumers().get().isEmpty();
        }
    }

    @BeforeClass
    public static void setupConfigsAndUtils() throws Exception {
        zkUtils = ZkUtils.apply(CLUSTER.zKConnectString(),
                30000,
                30000,
                JaasUtils.isZkSecurityEnabled());
        adminClient = AdminClient.createSimplePlaintext(CLUSTER.bootstrapServers());

        TestUtils.waitForCondition(new TopicsGotDeletedCondition(INPUT_TOPIC_1), 120000, "Topic " + INPUT_TOPIC_1 + " not deleted after 120 seconds.");
        TestUtils.waitForCondition(new TopicsGotDeletedCondition(INPUT_TOPIC_2), 120000, "Topic " + INPUT_TOPIC_2 + " not deleted after 120 seconds.");

        CLUSTER.createTopic(INPUT_TOPIC_1, 2, 1);
        CLUSTER.createTopic(INPUT_TOPIC_2, 3, 1);

        final Properties producerConfig = getProducerConfig();
        MockTime ts = CLUSTER.time;

        // use odd number of records to ensure that both partitions do have different number of records
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1, Collections.singleton(new KeyValue<>(1L, "A")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1, Collections.singleton(new KeyValue<>(2L, "B")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1, Collections.singleton(new KeyValue<>(3L, "C")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1, Collections.singleton(new KeyValue<>(4L, "D")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1, Collections.singleton(new KeyValue<>(5L, "E")), producerConfig, ts.milliseconds());

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_2, Collections.singleton(new KeyValue<>(10L, "a")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_2, Collections.singleton(new KeyValue<>(20L, "b")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_2, Collections.singleton(new KeyValue<>(30L, "c")), producerConfig, ts.milliseconds());

        Collection<KeyValue> expectedResult = new HashSet<>();
        expectedResult.add(new KeyValue<>(1L, "A"));
        expectedResult.add(new KeyValue<>(2L, "B"));
        expectedResult.add(new KeyValue<>(3L, "C"));
        expectedResult.add(new KeyValue<>(4L, "D"));
        expectedResult.add(new KeyValue<>(5L, "E"));
        expectedResult.add(new KeyValue<>(10L, "a"));
        expectedResult.add(new KeyValue<>(20L, "b"));
        expectedResult.add(new KeyValue<>(30L, "c"));
    }

    @AfterClass
    public static void release() {
        if (zkUtils != null) {
            zkUtils.close();
        }
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
    }

    @Before
    public void prepareTopology() throws Exception {
        TestUtils.waitForCondition(new TopicsGotDeletedCondition(OUTPUT_TOPIC), 120000, "Topic " + OUTPUT_TOPIC + " not deleted after 120 seconds.");
        CLUSTER.createTopic(OUTPUT_TOPIC);
    }

    @After
    public void cleanup() throws Exception {
        CLUSTER.deleteTopic(OUTPUT_TOPIC);
    }

    private static Properties getProducerConfig() {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return producerConfig;
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testStopSimpleTopology() throws Exception {
        final String appId = "stopSimpleToplology";

        final KStreamBuilder builder = new KStreamBuilder();
        builder
            .stream(Pattern.compile(INPUT_TOPIC_1 + "|" + INPUT_TOPIC_2))
            .to(OUTPUT_TOPIC);

        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.AUTOSTOP_AT_CONFIG, "eol");
        final Properties streamsConfig = StreamsTestUtils.getStreamsConfig(
                appId,
                CLUSTER.bootstrapServers(),
                Serdes.String().getClass().getName(),
                Serdes.Long().getClass().getName(),
                properties);
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);

        final KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(streamsConfig));
        streams.start();

        // verify that metadata topic contains expected offsets
        List<KeyValue<String, Long>> offsets = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(TestUtils.consumerConfig(CLUSTER.bootstrapServers(), Serdes.String().deserializer().getClass(), Serdes.Long().deserializer().getClass()), appId + "-METADATA", 6);
        final List<KeyValue<String, Long>> expectedResult = Arrays.asList(
            new KeyValue<>(INPUT_TOPIC_1 + "-0", 3L),
            new KeyValue<>(INPUT_TOPIC_1 + "-1", 2L),
            new KeyValue<>(INPUT_TOPIC_2 + "-0", 2L),
            new KeyValue<>(INPUT_TOPIC_2 + "-1", 2L),
            new KeyValue<>(INPUT_TOPIC_2 + "-2", 2L),
            new KeyValue<>(appId, 0L)
        );

        final Properties producerConfig = getProducerConfig();
        MockTime ts = CLUSTER.time;
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1, Collections.singleton(new KeyValue<>(6L, "F")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1, Collections.singleton(new KeyValue<>(7L, "G")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_2, Collections.singleton(new KeyValue<>(40L, "d")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_2, Collections.singleton(new KeyValue<>(50L, "e")), producerConfig, ts.milliseconds());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_2, Collections.singleton(new KeyValue<>(60L, "f")), producerConfig, ts.milliseconds());

        streams.close();
        Assert.fail();
        TestUtils.waitForCondition(new WaitUntilConsumerGroupGotClosed(appId), TIMEOUT_MULTIPLYER * CLEANUP_CONSUMER_TIMEOUT,
                "Reset Tool consumer group did not time out after " + (TIMEOUT_MULTIPLYER * CLEANUP_CONSUMER_TIMEOUT) + " ms.");
    }

    @Test
    public void testStopComplexTopology() {
        Assert.fail();
    }

    @Test
    public void testStopAndResumeComplexTopology() {
        Assert.fail();
    }

    @Test
    public void switchBackToStreamingMode() {
        Assert.fail();
    }
}
