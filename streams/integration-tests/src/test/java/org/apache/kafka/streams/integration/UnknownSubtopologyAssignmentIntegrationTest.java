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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.UnknownSubtopologyAssignor;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Asserts how a Kafka Streams client ought to behave when the group coordinator hands it an assignment naming a
 * subtopology it does not have, and therefore <strong>fails against a client that does not check for one</strong>.
 *
 * <p>A broker-side task assignor returns a {@code Map<String, Set<Integer>>} per member, so the subtopology id is an
 * arbitrary string that nothing in the assignor API constrains to name a real subtopology, and the broker does not
 * validate assignor output. {@link UnknownSubtopologyAssignor} therefore hands a member a task of a subtopology that
 * does not exist, targeting a member that runs nothing yet so that the bad assignment is the first it ever receives.
 *
 * <p>Whatever the client does with such an assignment, two things should hold. It should say so -- an assignment it
 * cannot apply is not something it can quietly ignore -- and the group as a whole should not silently stop consuming
 * an input partition. The test produces one batch of records before the second member joins and another one after, so
 * that a stall introduced by the bad assignment is visible as the second batch not being processed while the first one
 * was.
 *
 * <p>Without a check, none of that holds: resolving the assignment into topic partitions dereferences
 * {@code subtopologies().get(id)}, which is null for a subtopology the client does not have. The NPE is thrown from
 * {@code StreamsMembershipManager.poll()}, so the consumer network thread logs it once and
 * {@code markReconciliationInProgress()} -- already set, and only cleared on the asynchronous completion path -- makes
 * every later reconciliation return at the gate. The member then keeps heartbeating, its assignment only adds tasks so
 * the broker needs no acknowledgement, and nothing reaches the application: the client sits in REBALANCING, the group
 * reports STABLE, and the tasks that member was given are never run.
 */
@Timeout(600)
@Tag("integration")
public class UnknownSubtopologyAssignmentIntegrationTest {

    private static final int NUM_PARTITIONS = 2;
    private static final int RECORDS_BEFORE_SECOND_MEMBER = 50;
    private static final int NUM_RECORDS = 100;
    private static final Set<String> ALL_TASKS = Set.of("0_0", "0_1");
    private static final Set<Integer> PROCESSED_KEYS = ConcurrentHashMap.newKeySet();

    private static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties brokerProps = new Properties();
        brokerProps.put(
            GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNORS_CONFIG,
            UnknownSubtopologyAssignor.class.getName());
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "200");
        // A client that stops heartbeating is only fenced after the session timeout, and its tasks stay assigned to it
        // until then. The default of 45s is longer than this test is willing to wait for the group to recover.
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "6000");
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, "6000");
        cluster = new EmbeddedKafkaCluster(1, brokerProps);
        cluster.start();
        bootstrapServers = cluster.bootstrapServers();
    }

    @AfterAll
    public static void stopCluster() {
        cluster.stop();
        cluster = null;
    }

    @Test
    public void shouldReportAnUnusableAssignmentAndKeepConsumingEveryInputPartition() throws Exception {
        final String appId = "unknown-subtopology-app";
        final String inputTopic = appId + "-input";
        cluster.createTopic(inputTopic, NUM_PARTITIONS, 1);
        cluster.setGroupStreamsInitialRebalanceDelay(appId, 0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .foreach((key, value) -> PROCESSED_KEYS.add(key));

        final KafkaStreams streams1 = new KafkaStreams(builder.build(), streamsConfig(appId, "1"));
        final KafkaStreams streams2 = new KafkaStreams(builder.build(), streamsConfig(appId, "2"));

        try (final Admin admin = createAdmin()) {
            // Let the first client take both tasks before the second one joins, so that the second client is the one
            // running nothing when the assignment is computed, and the bad assignment is the first it ever receives.
            streams1.start();
            TestUtils.waitForCondition(
                () -> streams1.state() == KafkaStreams.State.RUNNING,
                60_000L,
                "Expected the first client to start running");

            // Establish that data flows at all, from both partitions, while one client holds both tasks.
            produceInputRecords(inputTopic, 0, RECORDS_BEFORE_SECOND_MEMBER);
            TestUtils.waitForCondition(
                () -> keysNotProcessed(0, RECORDS_BEFORE_SECOND_MEMBER) == 0,
                60_000L,
                "Expected the first client to process every record of the first batch");

            streams2.start();
            // Wait for the first client to actually hand one of its two tasks over, rather than merely for the second
            // client to join. Producing as soon as the group has two members races with the handover: while the first
            // client still runs both tasks it consumes every partition, so the second batch would be processed in full
            // and the stall would be invisible.
            TestUtils.waitForCondition(
                () -> runningTaskIds(streams1).size() == 1 || streams2.state() == KafkaStreams.State.ERROR,
                60_000L,
                "Expected the first client to give up one of its two tasks to the second client");

            // Produce again now that the bad assignment has been handed out, so that a stall it introduces shows up as
            // this batch not being processed even though the first one was.
            produceInputRecords(inputTopic, RECORDS_BEFORE_SECOND_MEMBER, NUM_RECORDS);

            TestUtils.waitForCondition(
                () -> {
                    final KafkaStreams.State stateOfFirst = streams1.state();
                    final KafkaStreams.State stateOfSecond = streams2.state();
                    final Set<String> assignedTasks = assignedTaskIds(admin, appId);
                    final Set<String> runningTasks = runningTaskIds(streams1, streams2);
                    final int missingFromFirstBatch = keysNotProcessed(0, RECORDS_BEFORE_SECOND_MEMBER);
                    final int missingFromSecondBatch = keysNotProcessed(RECORDS_BEFORE_SECOND_MEMBER, NUM_RECORDS);

                    final boolean shouldGoIntoErrorState =
                        stateOfFirst == KafkaStreams.State.ERROR || stateOfSecond == KafkaStreams.State.ERROR;
                    if (!shouldGoIntoErrorState) {
                        System.out.println("[unmet] A client should report an assignment it cannot apply as an error, "
                            + "but the client states are " + stateOfFirst + " and " + stateOfSecond);
                    }

                    final boolean shouldNotBeStuckRebalancing =
                        stateOfFirst != KafkaStreams.State.REBALANCING && stateOfSecond != KafkaStreams.State.REBALANCING;
                    if (!shouldNotBeStuckRebalancing) {
                        System.out.println("[unmet] No client should be left waiting to rebalance, "
                            + "but the client states are " + stateOfFirst + " and " + stateOfSecond);
                    }

                    final boolean shouldAssignEveryTask = assignedTasks.containsAll(ALL_TASKS);
                    if (!shouldAssignEveryTask) {
                        System.out.println("[unmet] Every task of the topology should be assigned to some member, "
                            + "but the group assignment is " + assignedTasks);
                    }

                    // A task the broker assigned but that no client is running is the whole failure mode: the group
                    // looks complete from the outside while an input partition is not being consumed by anyone.
                    final boolean shouldRunEveryAssignedTask = runningTasks.containsAll(ALL_TASKS);
                    if (!shouldRunEveryAssignedTask) {
                        System.out.println("[unmet] Every assigned task should be running on some client, but the "
                            + "clients are running " + runningTasks + " while the group assignment is "
                            + assignedTasks);
                    }

                    final boolean shouldKeepDataProducedBeforeTheSecondMember = missingFromFirstBatch == 0;
                    if (!shouldKeepDataProducedBeforeTheSecondMember) {
                        System.out.println("[unmet] The records produced before the second client joined should all "
                            + "have been processed, but " + missingFromFirstBatch + " of "
                            + RECORDS_BEFORE_SECOND_MEMBER + " are missing");
                    }

                    final boolean shouldProcessDataProducedAfterTheSecondMember = missingFromSecondBatch == 0;
                    if (!shouldProcessDataProducedAfterTheSecondMember) {
                        System.out.println("[unmet] Every input partition should still be consumed after the second "
                            + "client joined, but " + missingFromSecondBatch + " of "
                            + (NUM_RECORDS - RECORDS_BEFORE_SECOND_MEMBER) + " records produced since then are "
                            + "missing");
                    }

                    return shouldGoIntoErrorState
                        && shouldNotBeStuckRebalancing
                        && shouldAssignEveryTask
                        && shouldRunEveryAssignedTask
                        && shouldKeepDataProducedBeforeTheSecondMember
                        && shouldProcessDataProducedAfterTheSecondMember;
                },
                30_000L,
                3_000L,
                () -> "A client that is handed a task of a subtopology it does not have should report that, and no "
                    + "input partition should stop being consumed because of it"
            );
        } finally {
            streams1.close(Duration.ofSeconds(60));
            streams2.close(Duration.ofSeconds(60));
            PROCESSED_KEYS.clear();
        }
    }

    /**
     * The tasks the clients actually run, as opposed to the ones the broker assigned. Taken from the local thread
     * metadata of each client, which lists the tasks a StreamThread has created, so a task that was assigned but never
     * started does not appear here.
     */
    private static Set<String> runningTaskIds(final KafkaStreams... instances) {
        final Set<String> taskIds = new TreeSet<>();
        for (final KafkaStreams instance : instances) {
            for (final ThreadMetadata threadMetadata : instance.metadataForLocalThreads()) {
                for (final TaskMetadata taskMetadata : threadMetadata.activeTasks()) {
                    taskIds.add(taskMetadata.taskId().toString());
                }
            }
        }
        return taskIds;
    }

    private static Set<String> assignedTaskIds(final Admin admin, final String appId) throws Exception {
        final Set<String> taskIds = new TreeSet<>();
        for (final StreamsGroupMemberDescription member : describeGroup(admin, appId).members()) {
            for (final StreamsGroupMemberAssignment.TaskIds tasks : member.assignment().activeTasks()) {
                for (final int partition : tasks.partitions()) {
                    taskIds.add(tasks.subtopologyId() + "_" + partition);
                }
            }
        }
        return taskIds;
    }

    private static int keysNotProcessed(final int fromInclusive, final int toExclusive) {
        int missing = 0;
        for (int key = fromInclusive; key < toExclusive; key++) {
            if (!PROCESSED_KEYS.contains(key)) {
                missing++;
            }
        }
        return missing;
    }

    private static Admin createAdmin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    private static StreamsGroupDescription describeGroup(final Admin admin, final String appId) throws Exception {
        return admin.describeStreamsGroups(List.of(appId)).describedGroups().get(appId).get();
    }

    private static void produceInputRecords(final String inputTopic,
                                            final int fromInclusive,
                                            final int toExclusive) throws Exception {
        final List<KeyValue<Integer, Integer>> records = new ArrayList<>(toExclusive - fromInclusive);
        for (int key = fromInclusive; key < toExclusive; key++) {
            records.add(KeyValue.pair(key, key));
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic,
            records,
            TestUtils.producerConfig(bootstrapServers, IntegerSerializer.class, IntegerSerializer.class),
            cluster.time
        );
    }

    private static Properties streamsConfig(final String appId, final String instanceSuffix) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId + "-" + instanceSuffix).getPath());
        config.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        return config;
    }
}
