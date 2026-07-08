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
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a standby task is recycled (not closed and re-created) when it is promoted to an
 * active task on the same instance, i.e., that the state of an in-memory store survives the
 * promotion without a full changelog restore (cf. KAFKA-9501).
 */
@Timeout(600)
@Tag("integration")
public class StandbyTaskPromotionIntegrationTest {
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final String STORE_NAME = "count-store";
    private static final int NUM_RECORDS = 100;

    private String safeTestName;
    private String inputTopic;
    private String outputTopic;
    private int verificationGroupCounter = 0;

    private KafkaStreams client1;
    private KafkaStreams client2;

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws InterruptedException {
        safeTestName = safeUniqueTestName(testInfo);
        inputTopic = "input-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        CLUSTER.createTopic(inputTopic, 1, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);
    }

    private static class RecordingStandbyUpdateListener implements StandbyUpdateListener {
        final List<String> events = new CopyOnWriteArrayList<>();
        final List<SuspendReason> suspendReasons = new CopyOnWriteArrayList<>();
        final AtomicLong lastLoadedEndOffset = new AtomicLong(-1L);

        @Override
        public void onUpdateStart(final TopicPartition topicPartition, final String storeName, final long startingOffset) {
            events.add(String.format("onUpdateStart(%s, %s, startingOffset=%d)", topicPartition, storeName, startingOffset));
        }

        @Override
        public void onBatchLoaded(final TopicPartition topicPartition,
                                  final String storeName,
                                  final TaskId taskId,
                                  final long batchEndOffset,
                                  final long batchSize,
                                  final long currentEndOffset) {
            lastLoadedEndOffset.set(batchEndOffset);
            events.add(String.format("onBatchLoaded(%s, %s, batchEndOffset=%d, batchSize=%d, currentEndOffset=%d)",
                topicPartition, storeName, batchEndOffset, batchSize, currentEndOffset));
        }

        @Override
        public void onUpdateSuspended(final TopicPartition topicPartition,
                                      final String storeName,
                                      final long storeOffset,
                                      final long currentEndOffset,
                                      final SuspendReason reason) {
            suspendReasons.add(reason);
            events.add(String.format("onUpdateSuspended(%s, %s, storeOffset=%d, currentEndOffset=%d, reason=%s)",
                topicPartition, storeName, storeOffset, currentEndOffset, reason));
        }
    }

    private static class RecordingRestoreListener implements StateRestoreListener {
        final List<String> events = new CopyOnWriteArrayList<>();
        final List<Long> restoreStartOffsets = new CopyOnWriteArrayList<>();
        final AtomicLong totalRestored = new AtomicLong(0L);

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            restoreStartOffsets.add(startingOffset);
            events.add(String.format("onRestoreStart(%s, %s, startingOffset=%d, endingOffset=%d)",
                topicPartition, storeName, startingOffset, endingOffset));
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
            events.add(String.format("onBatchRestored(%s, %s, batchEndOffset=%d, numRestored=%d)",
                topicPartition, storeName, batchEndOffset, numRestored));
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestoredForPartition) {
            totalRestored.addAndGet(totalRestoredForPartition);
            events.add(String.format("onRestoreEnd(%s, %s, totalRestored=%d)", topicPartition, storeName, totalRestoredForPartition));
        }
    }

    private Properties streamsConfiguration(final boolean streamsProtocolEnabled) {
        final String appId = "app-" + safeTestName;
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        if (streamsProtocolEnabled) {
            streamsConfiguration.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
            CLUSTER.setGroupStandbyReplicas(appId, 1);
        } else {
            streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
            // only allow fully caught-up clients to own the active task, so the task stays on
            // client 1 when the empty client 2 joins the group
            streamsConfiguration.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 0L);
//            // classic Streams does not leave the group on close, so the promotion after closing
//            // client 1 only happens once the session times out
//            streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 10000);
//            streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 2000);
        }
        return streamsConfiguration;
    }

    private Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .groupByKey()
            .count(Materialized.<Integer, Long>as(Stores.inMemoryKeyValueStore(STORE_NAME))
                .withCachingDisabled())
            .toStream()
            .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.Long()));
        return builder.build();
    }

    @AfterEach
    public void tearDown() {
        if (client1 != null) {
            client1.close(Duration.ofSeconds(60));
        }
        if (client2 != null) {
            client2.close(Duration.ofSeconds(60));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldRecycleStandbyTaskOnPromotion(final boolean streamsProtocolEnabled) throws Exception {
        client1 = new KafkaStreams(topology(), streamsConfiguration(streamsProtocolEnabled));
        client2 = new KafkaStreams(topology(), streamsConfiguration(streamsProtocolEnabled));

        final RecordingStandbyUpdateListener standbyListener = new RecordingStandbyUpdateListener();
        final RecordingRestoreListener restoreListener = new RecordingRestoreListener();
        client2.setStandbyUpdateListener(standbyListener);
        client2.setGlobalStateRestoreListener(restoreListener);

        // client 1 runs alone first, so it owns the single active task
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(List.of(client1), Duration.ofSeconds(60));

        produceRecordsWithDistinctKeys();
        waitUntilOutputRecordsReceived(NUM_RECORDS);

        // client 2 joins; the sticky/HA assignment keeps the active task on client 1
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(List.of(client2), Duration.ofSeconds(60));
        TestUtils.waitForCondition(
            () -> hasActiveTask(client1) && !hasActiveTask(client2) && hasStandbyTask(client2),
            "Client 1 should own the active task and client 2 the standby task."
        );
        TestUtils.waitForCondition(
            () -> standbyListener.lastLoadedEndOffset.get() >= NUM_RECORDS - 1,
            "The standby task on client 2 should catch up to the end of the changelog."
        );

        // graceful shutdown of client 1 promotes the standby on client 2 to active
        client1.close(CloseOptions
            .timeout(Duration.ofSeconds(60))
            .withGroupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
        );
        TestUtils.waitForCondition(
            () -> hasActiveTask(client2),
            60_000L,
            "The standby task should be promoted to an active task on client 2."
        );

        // process a second batch to prove the promoted task is fully up and running
        produceRecordsWithDistinctKeys();
        waitUntilOutputRecordsReceived(2 * NUM_RECORDS);

        final String observedEvents = "Standby update events: " + standbyListener.events
            + "; restore events: " + restoreListener.events;
        assertEquals(
            List.of(StandbyUpdateListener.SuspendReason.PROMOTED),
            standbyListener.suspendReasons,
            "The standby task should be recycled, not closed. " + observedEvents
        );
        assertEquals(
            0L,
            restoreListener.totalRestored.get(),
            "The promoted task should not restore anything from the changelog. " + observedEvents
        );
        assertTrue(
            restoreListener.restoreStartOffsets.stream().noneMatch(offset -> offset == 0L),
            "The promoted task should not restore from the beginning of the changelog. " + observedEvents
        );
    }

    private void produceRecordsWithDistinctKeys() {
        final List<KeyValue<Integer, Integer>> records = new ArrayList<>(NUM_RECORDS);
        for (int key = 0; key < NUM_RECORDS; key++) {
            records.add(KeyValue.pair(key, 1));
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic,
            records,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), IntegerSerializer.class, IntegerSerializer.class),
            CLUSTER.time
        );
    }

    private void waitUntilOutputRecordsReceived(final int expectedNumRecords) throws Exception {
        // fresh group per call, so the verification consumer always reads the output topic from the beginning
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                "verify-" + safeTestName + "-" + verificationGroupCounter++,
                IntegerDeserializer.class,
                LongDeserializer.class
            ),
            outputTopic,
            expectedNumRecords
        );
    }

    private static boolean hasActiveTask(final KafkaStreams client) {
        return client.metadataForLocalThreads().stream().anyMatch(thread -> !thread.activeTasks().isEmpty());
    }

    private static boolean hasStandbyTask(final KafkaStreams client) {
        return client.metadataForLocalThreads().stream().anyMatch(thread -> !thread.standbyTasks().isEmpty());
    }
}
