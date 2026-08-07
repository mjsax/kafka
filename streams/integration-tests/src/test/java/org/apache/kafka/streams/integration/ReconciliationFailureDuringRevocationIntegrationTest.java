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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.GroupState;
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
 * Asserts that a Streams group recovers when reconciling an assignment fails on a member that has to give a task up,
 * and therefore <strong>fails against a client that lets such a failure go unhandled</strong>.
 *
 * <p>Where {@link UnknownSubtopologyAssignmentIntegrationTest} feeds the client a malformed assignment, this test needs
 * no malformed input at all: the assignment is the ordinary one the built-in assignor produces when a second client
 * joins a group whose single member holds every task. What is injected instead is a failure inside the reconciliation
 * itself, standing in for any defect on that path -- the exact defect does not matter, only that the reconciliation of
 * a revoking assignment does not complete.
 *
 * <p>Reaching this state requires injecting the failure rather than assigning something strange, because the
 * coordinator withholds additions while a revocation is outstanding ({@code CurrentAssignmentBuilder}), so a member is
 * never asked to revoke and to take on something new in the same assignment.
 *
 * <p>It matters because of what the coordinator does with a member that owes a revocation: it is
 * {@code UNREVOKED_TASKS}, so the rebalance timeout is armed against it and the member waiting for the revoked task
 * cannot start. When the timeout expires the member is fenced and rejoins -- which ought to be a clean slate, and is
 * where a client that merely leaves its reconciliation machinery in a bad state stays broken: the rejoin does not
 * reset it, so the member cannot reconcile its next assignment either, and the task it is given is never run.
 *
 * <p>Either resolution is acceptable to the assertions below: the member can fail and drop out of the group, or it can
 * recover and finish the rebalance. What is not acceptable is that the group never settles and an input partition
 * stops being consumed.
 */
@Timeout(600)
@Tag("integration")
public class ReconciliationFailureDuringRevocationIntegrationTest {

    private static final int NUM_PARTITIONS = 2;
    private static final int RECORDS_BEFORE_SECOND_MEMBER = 50;
    private static final int NUM_RECORDS = 100;
    private static final Set<String> ALL_TASKS = Set.of("0_0", "0_1");
    private static final Set<Integer> PROCESSED_KEYS = ConcurrentHashMap.newKeySet();

    private static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() throws IOException {
        // Arms the one-shot probe in StreamsMembershipManager.maybeReconcile. It fires on the first reconciliation
        // that has to revoke something and never again, so the group is left to recover from a single failure.
        System.setProperty("kafka20860.failOnceWhileRevoking", "true");
        final Properties brokerProps = new Properties();
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "200");
        // A member that stops heartbeating is only fenced after the session timeout, and the default of 45s is longer
        // than this test is willing to wait for a member that failed cleanly to leave the group.
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, "6000");
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, "6000");
        cluster = new EmbeddedKafkaCluster(1, brokerProps);
        cluster.start();
        bootstrapServers = cluster.bootstrapServers();
    }

    @AfterAll
    public static void stopCluster() {
        System.clearProperty("kafka20860.failOnceWhileRevoking");
        cluster.stop();
        cluster = null;
    }

    @Test
    public void shouldSettleAgainWhenReconcilingARevokingAssignmentFails() throws Exception {
        final String appId = "reconciliation-failure-revocation-app";
        final String inputTopic = appId + "-input";
        cluster.createTopic(inputTopic, NUM_PARTITIONS, 1);
        cluster.setGroupStreamsInitialRebalanceDelay(appId, 0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .foreach((key, value) -> PROCESSED_KEYS.add(key));

        final KafkaStreams streams1 = new KafkaStreams(builder.build(), streamsConfig(appId, "1"));
        final KafkaStreams streams2 = new KafkaStreams(builder.build(), streamsConfig(appId, "2"));

        try (final Admin admin = createAdmin()) {
            // The first client takes every task, so that the second one joining forces it to give one up.
            streams1.start();
            TestUtils.waitForCondition(
                () -> streams1.state() == KafkaStreams.State.RUNNING
                    && runningTaskIds(streams1).containsAll(ALL_TASKS),
                60_000L,
                "Expected the first client to start running and hold every task");

            produceInputRecords(inputTopic, 0, RECORDS_BEFORE_SECOND_MEMBER);
            TestUtils.waitForCondition(
                () -> keysNotProcessed(0, RECORDS_BEFORE_SECOND_MEMBER) == 0,
                60_000L,
                "Expected the first client to process every record of the first batch");

            streams2.start();
            TestUtils.waitForCondition(
                () -> describeGroup(admin, appId).members().size() == 2,
                60_000L,
                "Expected the second client to join the group");

            produceInputRecords(inputTopic, RECORDS_BEFORE_SECOND_MEMBER, NUM_RECORDS);

            TestUtils.waitForCondition(
                () -> {
                    final KafkaStreams.State stateOfFirst = streams1.state();
                    final KafkaStreams.State stateOfSecond = streams2.state();
                    final GroupState groupState = groupState(admin, appId);
                    final Set<String> assignedTasks = assignedTaskIds(admin, appId);
                    final Set<String> runningTasks = runningTaskIds(streams1, streams2);
                    final int missingRecords = keysNotProcessed(0, NUM_RECORDS);

                    // Failing outright and leaving the group is an acceptable outcome, so ERROR is not counted against
                    // the client here; being left waiting to rebalance is, because nobody makes progress from there.
                    final boolean shouldNotBeStuckRebalancing =
                        stateOfFirst != KafkaStreams.State.REBALANCING && stateOfSecond != KafkaStreams.State.REBALANCING;
                    if (!shouldNotBeStuckRebalancing) {
                        System.out.println("[unmet] No client should be left waiting to rebalance, "
                            + "but the client states are " + stateOfFirst + " and " + stateOfSecond);
                    }

                    final boolean shouldReachStableGroup = groupState == GroupState.STABLE;
                    if (!shouldReachStableGroup) {
                        System.out.println("[unmet] The group should finish rebalancing and settle, but it is in state "
                            + groupState);
                    }

                    final boolean shouldAssignEveryTask = assignedTasks.containsAll(ALL_TASKS);
                    if (!shouldAssignEveryTask) {
                        System.out.println("[unmet] Every task of the topology should be assigned to some member, "
                            + "but the group assignment is " + assignedTasks);
                    }

                    final boolean shouldRunEveryAssignedTask = runningTasks.containsAll(ALL_TASKS);
                    if (!shouldRunEveryAssignedTask) {
                        System.out.println("[unmet] Every assigned task should be running on some client, but the "
                            + "clients are running " + runningTasks + " while the group assignment is "
                            + assignedTasks);
                    }

                    final boolean shouldProcessAllInput = missingRecords == 0;
                    if (!shouldProcessAllInput) {
                        System.out.println("[unmet] Every input record should be processed, but " + missingRecords
                            + " of " + NUM_RECORDS + " are missing");
                    }

                    return shouldNotBeStuckRebalancing
                        && shouldReachStableGroup
                        && shouldAssignEveryTask
                        && shouldRunEveryAssignedTask
                        && shouldProcessAllInput;
                },
                30_000L,
                3_000L,
                () -> "A member whose reconciliation fails while it owes a revocation should either fail and leave the "
                    + "group or recover, so that the rebalance completes and every input partition is consumed again"
            );
        } finally {
            streams1.close(Duration.ofSeconds(60));
            streams2.close(Duration.ofSeconds(60));
            PROCESSED_KEYS.clear();
        }
    }

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

    private static GroupState groupState(final Admin admin, final String appId) throws Exception {
        return describeGroup(admin, appId).groupState();
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
        // The client reports this as its rebalance timeout, which is what the coordinator arms against a member that
        // owes a revocation. The default of five minutes is far longer than this test can wait for the group to give
        // up on a member that never revokes.
        config.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), 10_000);
        return config;
    }
}
