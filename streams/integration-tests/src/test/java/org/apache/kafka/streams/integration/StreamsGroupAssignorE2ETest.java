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
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.StreamsGroupHeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.streams.assignor.TaskId;
import org.apache.kafka.coordinator.group.streams.assignor.TopologyDescriber;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.test.TestUtils;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end proof-of-concept that the KIP-1071 information exchanged between a real {@link KafkaStreams}
 * client and the broker reaches the broker-side {@link TaskAssignor}, and that the configs the broker sends
 * back are stored on the client.
 *
 * <p>Verified in one run:
 * <ul>
 *     <li>client &rarr; broker &rarr; assignor: per-member {@code taskOffsets}/{@code taskEndOffsets} (reported via
 *     heartbeat) and member metadata show up in the {@link GroupSpec} passed to {@link TaskAssignor#assign};</li>
 *     <li>broker config &rarr; assignor: {@code num.standby.replicas} shows up in {@link GroupSpec#assignmentConfigs()};</li>
 *     <li>broker &rarr; client (response): {@code acceptableRecoveryLag}, {@code taskOffsetIntervalMs} and
 *     {@code heartbeatIntervalMs} are stored client-side in {@link StreamsRebalanceData}.</li>
 * </ul>
 *
 * <p><b>POC note:</b> the broker-side streams assignor is not yet pluggable (the proper fix — a
 * {@code group.streams.assignors} config mirroring {@code group.consumer.assignors} — is separate, not-yet-landed
 * work via KIP-1357). As an interim measure this test uses reflection to swap in a capturing assignor on the running
 * broker and to read internal coordinator/client state. Delete {@link CapturingTaskAssignor} and {@link Reflection}
 * once the pluggable-assignor feature lands.
 */
@Tag("integration")
@Timeout(600)
public class StreamsGroupAssignorE2ETest {

    private static final String STORE_NAME = "counts";
    private static final int NUM_STANDBY_REPLICAS = 1;
    private static final long ACCEPTABLE_RECOVERY_LAG = 4242L;
    private static final int TASK_OFFSET_INTERVAL_MS = 1_000;
    private static final int HEARTBEAT_INTERVAL_MS = 500;
    // Client-tag key marking a throwaway "churn" member: the injected assignor recognizes it, hides it from the
    // delegate sticky assignor (so the real members' tasks don't move), and gives it an empty assignment. This lets a
    // test bump the group epoch — forcing a fresh assignor invocation that captures current offsets — without a
    // rebalance that would disturb the placement under test.
    private static final String CHURN_TAG = "assignor-e2e-churn";
    // Streams config for a churn member: sets the CHURN_TAG client tag so the injected assignor hides it.
    private static final Map<String, Object> CHURN_MEMBER_PROPS =
        Map.of(StreamsConfig.CLIENT_TAG_PREFIX + CHURN_TAG, "true");

    private EmbeddedKafkaCluster cluster;
    private String inputTopic;
    private String outputTopic;
    private String applicationId;
    private CapturingTaskAssignor capturingAssignor;
    private final List<KafkaStreams> streamsInstances = new ArrayList<>();

    @BeforeEach
    public void setup(final TestInfo testInfo) {
        final String safeName = safeUniqueTestName(testInfo);
        applicationId = "app-" + safeName;
        inputTopic = "input-" + safeName;
        outputTopic = "output-" + safeName;
    }

    /**
     * Starts the embedded cluster (single coordinator shard), creates the topics, and injects the capturing
     * assignor before any KafkaStreams starts. {@code numStandbyReplicas} is configured per test: tests that need
     * standby tasks use 1; tests that need a free slot to force a warm-up use 0.
     */
    private void startCluster(final int numStandbyReplicas) throws Exception {
        final Properties brokerConfig = new Properties();
        // Single coordinator shard so there is exactly one GroupMetadataManager to inject into.
        brokerConfig.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1");
        brokerConfig.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        brokerConfig.put(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, Integer.toString(numStandbyReplicas));
        brokerConfig.put(GroupCoordinatorConfig.STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG_CONFIG, Long.toString(ACCEPTABLE_RECOVERY_LAG));
        brokerConfig.put(GroupCoordinatorConfig.STREAMS_GROUP_MIN_TASK_OFFSET_INTERVAL_MS_CONFIG, Integer.toString(TASK_OFFSET_INTERVAL_MS));
        brokerConfig.put(GroupCoordinatorConfig.STREAMS_GROUP_TASK_OFFSET_INTERVAL_MS_CONFIG, Integer.toString(TASK_OFFSET_INTERVAL_MS));
        brokerConfig.put(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, Integer.toString(HEARTBEAT_INTERVAL_MS));
        // Recompute the target assignment immediately on every group-epoch bump (no batching delay), so the
        // capturing assignor sees a fresh GroupSpec as soon as a member joins.
        brokerConfig.put(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, "0");

        cluster = new EmbeddedKafkaCluster(1, brokerConfig);
        cluster.start();

        cluster.createTopic(inputTopic, 2, 1);
        cluster.createTopic(outputTopic, 2, 1);

        // TODO: replace this later with a plugable assignor via KIP-1357
        capturingAssignor = new CapturingTaskAssignor();
        Reflection.injectStreamsAssignor(cluster, capturingAssignor);
    }

    @AfterEach
    public void shutdown() {
        for (final KafkaStreams streams : streamsInstances) {
            try {
                streams.close(Duration.ofSeconds(30L));
                streams.cleanUp();
            } catch (final Exception ignored) {
                // best effort
            }
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    public void shouldForwardOffsetsMetadataAndConfigsToAssignorAndConfigsBackToClient() throws Exception {
        startCluster(NUM_STANDBY_REPLICAS);
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as(STORE_NAME))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        // Controlled, deterministic input: distinct keys produced to explicit partitions so each record is exactly
        // one changelog write. Expected changelog end offsets: partition 0 -> RECORDS_P0, partition 1 -> RECORDS_P1.
        final int recordsP0 = 4;
        final int recordsP1 = 6;
        produceControlledInput(recordsP0, recordsP1);

        // Capture the client's heartbeat-response logging BEFORE any instance starts, so we can verify the response
        // leg (the configs the coordinator sends back) without reflecting into client internals. The client logs the
        // received config on first receipt.
        try (final LogCaptureAppender hbLog =
                 LogCaptureAppender.createAndRegister(StreamsGroupHeartbeatRequestManager.class)) {
            hbLog.setClassLogger(StreamsGroupHeartbeatRequestManager.class, Level.INFO);

            final KafkaStreams instanceA = startStreams(builder);
            final KafkaStreams instanceB = startStreams(builder);
            waitForRunning(instanceA);
            waitForRunning(instanceB);

            final String changelogTopic = applicationId + "-" + STORE_NAME + "-changelog";

            // Each record is one changelog write, so the changelog end offset of partition p equals the number of
            // records produced to input partition p. The two standby tasks (one per partition, on the two instances)
            // restore these changelogs and report offsets keyed by task (subtopology, partition).
            final Map<Integer, Long> expectedChangelogEnd = Map.of(0, (long) recordsP0, 1, (long) recordsP1);

            // Wait until the changelog is fully produced (deterministic end offsets, no processing race).
            TestUtils.waitForCondition(
                () -> changelogEnds(changelogTopic).equals(expectedChangelogEnd),
                120_000,
                "Changelog never reached the expected end offsets " + expectedChangelogEnd + "."
            );

            // Wait until BOTH standby tasks have fully restored and reported, then assert the reported offsets are
            // deterministic and matched to the correct task/partition.
            //
            // NOTE (off-by-one, intended for now -- TODO investigate): taskOffsetSum is the changelog *position*
            // (log-end-offset = record count N), while taskEndOffsetSum is the *last* offset (N-1). So a fully
            // caught-up task reports offset == N and endOffset == N-1 (assignor lag endOffset-offset = -1). The two
            // values come from different code paths (StateDirectory.taskOffsetSums vs logicalChangelogEndOffsets) with
            // different offset conventions; harmless against acceptable.recovery.lag but worth revisiting.
            // Reported offsets don't bump the group epoch, so the assignor is only re-invoked on membership changes.
            // Bounce a HIDDEN churn member (see CHURN_TAG) until a recompute captures a GroupSpec whose reported offsets
            // show BOTH standby partitions caught up, then assert the exact values from that captured spec. Hiding the
            // churn member keeps the two real members' standbys in place so they stay caught up — no broker-internal peeking.
            final GroupSpec spec = churnUntilAssignorObserves(
                builder,
                capturingAssignor,
                captured -> {
                    final Map<Integer, long[]> reported = reportedOffsetsByPartition(captured);
                    return matchesCaughtUp(reported.get(0), recordsP0) && matchesCaughtUp(reported.get(1), recordsP1);
                },
                "Both standby tasks did not report deterministic caught-up offsets matched to their changelog partitions.",
                CHURN_MEMBER_PROPS
            );

            final Map<Integer, long[]> reported = reportedOffsetsByPartition(spec);
            for (final int partition : new int[] {0, 1}) {
                final long expectedRecords = partition == 0 ? recordsP0 : recordsP1;
                final long[] offsetAndEnd = reported.get(partition);
                assertNotNull(offsetAndEnd, "No reported offsets for the standby task of partition " + partition + ".");
                assertEquals(expectedRecords, offsetAndEnd[0],
                    "Reported taskOffsetSum for partition " + partition + " must equal the changelog position.");
                assertEquals(expectedRecords - 1, offsetAndEnd[1],
                    "Reported taskEndOffsetSum for partition " + partition + " must equal the changelog last offset.");
            }

            // Member metadata is forwarded to the assignor too. Pick a member reporting offsets (a real member, never
            // the empty hidden churn member) and check its process id is present.
            final AssignmentMemberSpec memberWithOffsets = spec.members().values().stream()
                .filter(m -> !m.taskOffsets().isEmpty() && !m.taskEndOffsets().isEmpty())
                .findFirst()
                .orElseThrow();
            assertFalse(memberWithOffsets.processId().isEmpty(), "Expected a process id on the member spec.");

            // Broker config reached the assignor via the assignment configs.
            assertEquals(
                Integer.toString(NUM_STANDBY_REPLICAS),
                spec.assignmentConfigs().get("num.standby.replicas"),
                "Expected num.standby.replicas to reach the assignor via assignmentConfigs."
            );

            // Response leg: the client logged the configs the coordinator sent back (observed via the log, not by
            // reflecting into client-internal StreamsRebalanceData).
            TestUtils.waitForCondition(
                () -> hbLog.getMessages().stream().anyMatch(m ->
                    m.contains("heartbeatIntervalMs=" + HEARTBEAT_INTERVAL_MS)
                        && m.contains("taskOffsetIntervalMs=" + TASK_OFFSET_INTERVAL_MS)
                        && m.contains("acceptableRecoveryLag=" + ACCEPTABLE_RECOVERY_LAG)),
                120_000,
                "Client never logged the Streams group config received from the coordinator."
            );
        }
    }

    /** Collapses a captured spec's per-member reported offsets to partition -> [taskOffsetSum, taskEndOffsetSum]. */
    private static Map<Integer, long[]> reportedOffsetsByPartition(final GroupSpec spec) {
        final Map<Integer, long[]> byPartition = new HashMap<>();
        for (final AssignmentMemberSpec member : spec.members().values()) {
            member.taskOffsets().forEach((task, offset) -> {
                final Long endOffset = member.taskEndOffsets().get(task);
                if (endOffset != null) {
                    byPartition.put(task.partition(), new long[] {offset, endOffset});
                }
            });
        }
        return byPartition;
    }

    private static boolean matchesCaughtUp(final long[] offsetAndEnd, final long expectedRecords) {
        return offsetAndEnd != null
            && offsetAndEnd[0] == expectedRecords          // taskOffsetSum == changelog position (N)
            && offsetAndEnd[1] == expectedRecords - 1;     // taskEndOffsetSum == changelog last offset (N-1)
    }

    @Test
    public void shouldReportForcedWarmupTaskToAssignor() throws Exception {
        // num.standby.replicas=0 leaves a free slot: the injected assignor transform forces a warm-up of an active
        // task onto a member that does not own it. That member must create the warm-up, restore it from the
        // changelog, and report its offsets back to the broker/assignor.
        startCluster(0);
        capturingAssignor.setTransform(StreamsGroupAssignorE2ETest::injectWarmupTask);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as(STORE_NAME))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        produceInput();

        final KafkaStreams instanceA = startStreams(builder);
        final KafkaStreams instanceB = startStreams(builder);
        waitForRunning(instanceA);
        waitForRunning(instanceB);

        // The forced warm-up lives in the assignor's *output*; it only appears in a captured *input* spec after a
        // later assignment round (the client must first create the warm-up, restore it, and report its offsets). With
        // no standby replicas, the only tasks that report offsets are warm-ups. Reported offsets don't bump the group
        // epoch, so bounce an extra member until a recompute captures a spec where some member is BOTH assigned the
        // warm-up and reporting that task's offsets.
        churnUntilAssignorObserves(
            builder,
            capturingAssignor,
            StreamsGroupAssignorE2ETest::hasMemberWithReportedWarmup,
            "Assignor never saw a member both assigned a warm-up task and reporting that task's offsets."
        );
    }

    /** True if any member in the spec is assigned a warm-up task that also has a reported task offset. */
    private static boolean hasMemberWithReportedWarmup(final GroupSpec spec) {
        return spec.members().values().stream().anyMatch(member ->
            member.warmupTasks().entrySet().stream().anyMatch(warmup ->
                warmup.getValue().stream().anyMatch(partition ->
                    member.taskOffsets().keySet().stream().anyMatch(task ->
                        task.subtopologyId().equals(warmup.getKey()) && task.partition() == partition))));
    }

    @Test
    public void shouldObserveWarmupLagThroughControlledRestore() throws Exception {
        // num.standby.replicas=0 + a forced warm-up means the warm-up is the ONLY task that reports offsets
        // (running actives are excluded, no standbys), so any broker-stored offset is the warm-up's. Both instances
        // share a pausable restore consumer: while paused the warm-up cannot restore, so the broker observes it
        // behind (lag > 0, not caught up); after release it restores and the broker observes it caught up (lag <= 0).
        //
        // This is the e2e-observable facet of the "hot warm-up" condition: the LAG the broker/assignor sees. The
        // per-heartbeat send cadence ("hot => report every heartbeat") is covered by the staged unit test, as it has
        // no e2e observation seam.
        startCluster(0);
        capturingAssignor.setTransform(StreamsGroupAssignorE2ETest::injectWarmupTask);
        final PausableClientSupplier supplier = new PausableClientSupplier();
        supplier.pauseRestore(true);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as(STORE_NAME))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        produceControlledInput(4, 6);

        final KafkaStreams instanceA = startStreams(builder, TestUtils.tempDirectory().getAbsolutePath(), supplier);
        final KafkaStreams instanceB = startStreams(builder, TestUtils.tempDirectory().getAbsolutePath(), supplier);
        waitForRunning(instanceA);
        waitForRunning(instanceB);

        // While restore is paused, the warm-up cannot make progress: the assignor observes it not caught up (it has
        // restored nothing, so its reported offset is 0). A hidden churn member bumps the epoch to force fresh
        // captures without moving the warm-up off its (paused-restore) instance.
        churnUntilAssignorObserves(
            builder,
            capturingAssignor,
            spec -> reportsTaskWithOffset(spec, 0L),
            "Paused warm-up never reported a not-caught-up offset (0) to the assignor.",
            CHURN_MEMBER_PROPS
        );

        // Releasing restore lets the warm-up catch up: the assignor now observes it caught up (its reported offset
        // reaches the changelog end; the off-by-one means offset == end + 1, i.e. lag <= 0).
        supplier.pauseRestore(false);
        churnUntilAssignorObserves(
            builder,
            capturingAssignor,
            this::reportsCaughtUpTask,
            "Warm-up never reported as caught up after restore was released.",
            CHURN_MEMBER_PROPS
        );
    }

    @Test
    public void shouldReportOnDiskStateOnStartup() throws Exception {
        // "On startup, if local state is found, the task-offset-sum is reported." A restoring task whose on-disk
        // (checkpoint) offset is behind the changelog only arises via a handoff: build state on two instances, stop
        // one, let the other advance the changelog, then restart the stopped one with restore PAUSED so its task
        // stays at the on-disk offset. The broker must then observe that stale on-disk offset (0 < offset < end).
        startCluster(NUM_STANDBY_REPLICAS);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as(STORE_NAME))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final int recordsP0 = 4;
        final int recordsP1 = 6;
        produceControlledInput(recordsP0, recordsP1);

        final String changelogTopic = applicationId + "-" + STORE_NAME + "-changelog";
        final String stateDirB = TestUtils.tempDirectory().getAbsolutePath();

        final KafkaStreams instanceA = startStreams(builder);
        final KafkaStreams instanceB = startStreams(builder, stateDirB, null);
        waitForRunning(instanceA);
        waitForRunning(instanceB);

        // Both instances are caught up and have checkpointed local state for their tasks.
        TestUtils.waitForCondition(
            () -> changelogEnds(changelogTopic).equals(Map.of(0, (long) recordsP0, 1, (long) recordsP1)),
            120_000,
            "Changelog never reached the initial end offsets."
        );

        // Stop B (keep its state dir) and let A (now active for both partitions) advance the changelog.
        instanceB.close(Duration.ofSeconds(30L));
        final int deltaP0 = 3;
        final int deltaP1 = 3;
        produceControlledInput(deltaP0, deltaP1);
        TestUtils.waitForCondition(
            () -> changelogEnds(changelogTopic).equals(Map.of(0, (long) (recordsP0 + deltaP0), 1, (long) (recordsP1 + deltaP1))),
            120_000,
            "Changelog never reached the advanced end offsets after B was stopped."
        );

        // Restart B reusing its state dir, with restore PAUSED: B finds local state (the old checkpoint) but cannot
        // catch up, so it stays at the on-disk offset.
        final PausableClientSupplier supplier = new PausableClientSupplier();
        supplier.pauseRestore(true);
        startStreams(builder, stateDirB, supplier);

        // The broker must observe B's on-disk (checkpoint) offset for some partition: offset == the pre-delta
        // changelog end, end-offset == the current changelog last offset (off-by-one), i.e. local state was found
        // and reported on startup while still behind.
        // The restarted instance found its local state and reported its on-disk (checkpoint) offset sums on startup
        // — the pre-delta values (recordsP0, recordsP1) — even though the changelog has since advanced beyond them.
        // A hidden churn member bumps the epoch to force fresh captures without moving B's tasks (which would let it
        // resume restoring off its paused consumer or drop the local state we want reported).
        churnUntilAssignorObserves(
            builder,
            capturingAssignor,
            spec -> reportsStaleOnDiskState(spec, recordsP0, recordsP1),
            "Restarted instance never reported its on-disk (checkpoint) offsets on startup.",
            CHURN_MEMBER_PROPS
        );
    }

    @Test
    public void shouldReportCommittedEndOffsetForSourceTopicOptimizedStore() throws Exception {
        // A store materialized directly from a source topic with REUSE_KTABLE_SOURCE_TOPICS reuses the source topic
        // AS the changelog (no separate changelog topic). For such stores the reported task END offset is the
        // *committed* (logical log-end-)offset on the source topic -- min(physicalEnd, committed) -- not the physical
        // log-end-offset. We make committed < physical by letting the active commit M records, then pausing
        // processing (KafkaStreams#pause keeps the member heartbeating but stops consumption, so the committed
        // offset freezes) while we append more records to the source topic; the standby then reports end == committed.
        //
        // Processing is paused via KafkaStreams#pause rather than a client-supplier consumer override because the
        // main consumer under the streams group protocol is the async consumer, which is not supplied via
        // KafkaClientSupplier. A minimal input buffer keeps any prefetch from advancing the committed offset.
        startCluster(NUM_STANDBY_REPLICAS);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Materialized.as(STORE_NAME));

        final Map<String, Object> extraProps = Map.of(
            StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS,
            StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 1,
            StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 1
        );

        final int committedRecords = 5;
        produceControlledInput(committedRecords, committedRecords); // both partitions

        final KafkaStreams instanceA = startStreams(builder, TestUtils.tempDirectory().getAbsolutePath(), null, extraProps);
        final KafkaStreams instanceB = startStreams(builder, TestUtils.tempDirectory().getAbsolutePath(), null, extraProps);
        waitForRunning(instanceA);
        waitForRunning(instanceB);

        // Wait until both partitions are fully committed (each instance is active for one).
        TestUtils.waitForCondition(
            () -> committedOffset(inputTopic, 0) == committedRecords && committedOffset(inputTopic, 1) == committedRecords,
            120_000,
            "Active tasks never committed the initial records on the source topic."
        );

        // Pause ONLY instanceA (it keeps heartbeating, so no rebalance and it retains its active task). The partition
        // it is active for now has a frozen committed offset, while instanceB (still running) is the standby for that
        // same partition and keeps reporting its logical end-offset. Appending more records advances the physical end
        // past the frozen committed offset for that partition.
        instanceA.pause();
        final int extraRecords = 5;
        produceControlledInput(extraRecords, extraRecords);
        final long physicalEnd = committedRecords + extraRecords;
        TestUtils.waitForCondition(
            () -> topicEnd(inputTopic, 0) == physicalEnd && topicEnd(inputTopic, 1) == physicalEnd,
            120_000,
            "Source topic physical ends never reached the expected value."
        );

        // For the partition instanceA is active for, the committed offset stays frozen below the physical end, and
        // instanceB's standby reports the COMMITTED (logical) end-offset for it — min(physical, committed) — i.e. the
        // reported end tracks committed (== committed or committed-1 off-by-one) and is strictly below the physical end.
        // A hidden churn member bumps the epoch to force fresh captures without moving instanceA's (paused) active task
        // — which would unfreeze the committed offset. The committed offset is still read live (Admin, not reflection).
        churnUntilAssignorObserves(
            builder,
            capturingAssignor,
            spec -> {
                for (final int partition : new int[] {0, 1}) {
                    final long committed;
                    try {
                        committed = committedOffset(inputTopic, partition);
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                    if (committed < physicalEnd && reportsLogicalEndOffset(spec, partition, committed, physicalEnd)) {
                        return true;
                    }
                }
                return false;
            },
            "Standby never reported the committed (logical) end offset for the source-topic-optimized store.",
            CHURN_MEMBER_PROPS
        );
    }

    /** True if some member in the captured spec reports partition {@code p} with an end-offset that tracks committed
     *  (committed or committed-1) and is strictly below the physical end — i.e. the logical (committed) end, not the physical end. */
    private static boolean reportsLogicalEndOffset(final GroupSpec spec, final int partition, final long committed, final long physicalEnd) {
        for (final AssignmentMemberSpec member : spec.members().values()) {
            for (final Map.Entry<TaskId, Long> entry : member.taskEndOffsets().entrySet()) {
                final long end = entry.getValue();
                if (entry.getKey().partition() == partition
                    && end < physicalEnd
                    && (end == committed || end == committed - 1)) {
                    return true;
                }
            }
        }
        return false;
    }

    private long committedOffset(final String topic, final int partition) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            final var offsets = admin.listConsumerGroupOffsets(applicationId)
                .partitionsToOffsetAndMetadata().get();
            final var offsetAndMetadata = offsets.get(new TopicPartition(topic, partition));
            return offsetAndMetadata == null ? -1L : offsetAndMetadata.offset();
        }
    }

    private long topicEnd(final String topic, final int partition) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            return admin.listOffsets(Map.of(new TopicPartition(topic, partition), OffsetSpec.latest()))
                .all().get().get(new TopicPartition(topic, partition)).offset();
        }
    }

    /** True if some member in the captured spec reports a task whose offset equals {@code offset}. */
    private static boolean reportsTaskWithOffset(final GroupSpec spec, final long offset) {
        return spec.members().values().stream()
            .flatMap(member -> member.taskOffsets().values().stream())
            .anyMatch(reported -> reported == offset);
    }

    /** True if some member in the captured spec reports a task that is caught up: finite end-offset and offset >= end. */
    private boolean reportsCaughtUpTask(final GroupSpec spec) {
        for (final AssignmentMemberSpec member : spec.members().values()) {
            for (final Map.Entry<TaskId, Long> entry : member.taskOffsets().entrySet()) {
                final Long end = member.taskEndOffsets().get(entry.getKey());
                if (end != null && end != Long.MAX_VALUE && entry.getValue() >= end) {
                    return true;
                }
            }
        }
        return false;
    }

    /** True if some member in the captured spec reports BOTH partitions' on-disk (checkpoint) offsets: partition 0 == p0, partition 1 == p1. */
    private static boolean reportsStaleOnDiskState(final GroupSpec spec, final long p0, final long p1) {
        for (final AssignmentMemberSpec member : spec.members().values()) {
            Long offset0 = null;
            Long offset1 = null;
            for (final Map.Entry<TaskId, Long> entry : member.taskOffsets().entrySet()) {
                if (entry.getKey().partition() == 0) {
                    offset0 = entry.getValue();
                } else if (entry.getKey().partition() == 1) {
                    offset1 = entry.getValue();
                }
            }
            if (offset0 != null && offset0 == p0 && offset1 != null && offset1 == p1) {
                return true;
            }
        }
        return false;
    }

    /**
     * Forces a warm-up of an existing active task onto a member that does not already own it (no-op with fewer than
     * two members or no free slot). Keeps the assignment valid by reusing a real (subtopology, partition).
     */
    private static GroupAssignment injectWarmupTask(final GroupAssignment assignment) {
        final Map<String, MemberAssignment> members = new HashMap<>(assignment.members());
        if (members.size() < 2) {
            return assignment;
        }
        String taskSub = null;
        Integer taskPart = null;
        String activeOwner = null;
        for (final Map.Entry<String, MemberAssignment> entry : members.entrySet()) {
            for (final Map.Entry<String, Set<Integer>> active : entry.getValue().activeTasks().entrySet()) {
                if (!active.getValue().isEmpty()) {
                    taskSub = active.getKey();
                    taskPart = active.getValue().iterator().next();
                    activeOwner = entry.getKey();
                    break;
                }
            }
            if (taskSub != null) {
                break;
            }
        }
        if (taskSub == null) {
            return assignment;
        }
        final String sub = taskSub;
        final int part = taskPart;
        final String owner = activeOwner;
        final String target = members.keySet().stream()
            .filter(member -> !member.equals(owner))
            .filter(member -> !members.get(member).activeTasks().getOrDefault(sub, Set.of()).contains(part))
            .findFirst()
            .orElse(null);
        if (target == null) {
            return assignment;
        }
        final MemberAssignment old = members.get(target);
        final Map<String, Set<Integer>> newWarmup = new HashMap<>(old.warmupTasks());
        final Set<Integer> parts = new HashSet<>(newWarmup.getOrDefault(sub, Set.of()));
        parts.add(part);
        newWarmup.put(sub, parts);
        members.put(target, new MemberAssignment(old.activeTasks(), old.standbyTasks(), newWarmup));
        return new GroupAssignment(members);
    }

    private Map<Integer, Long> changelogEnds(final String changelogTopic) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            final Map<TopicPartition, OffsetSpec> request = Map.of(
                new TopicPartition(changelogTopic, 0), OffsetSpec.latest(),
                new TopicPartition(changelogTopic, 1), OffsetSpec.latest()
            );
            final Map<TopicPartition, ListOffsetsResultInfo> ends = admin.listOffsets(request).all().get();
            return Map.of(
                0, ends.get(new TopicPartition(changelogTopic, 0)).offset(),
                1, ends.get(new TopicPartition(changelogTopic, 1)).offset()
            );
        }
    }

    private void produceControlledInput(final int recordsP0, final int recordsP1) throws Exception {
        final Properties producerConfig =
            TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class);
        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            int key = 0;
            for (int i = 0; i < recordsP0; i++) {
                producer.send(new ProducerRecord<>(inputTopic, 0, "k-" + (key++), "v")).get();
            }
            for (int i = 0; i < recordsP1; i++) {
                producer.send(new ProducerRecord<>(inputTopic, 1, "k-" + (key++), "v")).get();
            }
        }
    }

    private void produceInput() {
        final List<KeyValue<String, String>> records = IntStream.range(0, 100)
            .mapToObj(i -> KeyValue.pair("key-" + (i % 8), "v" + i))
            .toList();
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic,
            records,
            TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class),
            cluster.time
        );
    }

    private KafkaStreams startStreams(final StreamsBuilder builder) {
        return startStreams(builder, TestUtils.tempDirectory().getAbsolutePath(), null);
    }

    private KafkaStreams startStreams(final StreamsBuilder builder,
                                      final String stateDir,
                                      final KafkaClientSupplier clientSupplier) {
        return startStreams(builder, stateDir, clientSupplier, Map.of());
    }

    private KafkaStreams startStreams(final StreamsBuilder builder,
                                      final String stateDir,
                                      final KafkaClientSupplier clientSupplier,
                                      final Map<String, Object> extraProps) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        // Disable record caching so every processed record is written to the changelog: this makes the changelog
        // offsets (and therefore the reported task offsets/end-offsets) deterministic from the known input.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(extraProps);

        final KafkaStreams streams = clientSupplier == null
            ? new KafkaStreams(builder.build(props), props)
            : new KafkaStreams(builder.build(props), props, clientSupplier);
        streamsInstances.add(streams);
        streams.start();
        return streams;
    }

    private void waitForRunning(final KafkaStreams streams) throws Exception {
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            120_000,
            "KafkaStreams instance did not reach RUNNING."
        );
    }

    /**
     * Drives the capturing assignor to observe reported task offsets. Reported task offsets are transient and do not
     * bump the group epoch, so the assignor is only re-invoked on membership changes.
     * This repeatedly bounces one extra member — each join, and each subsequent leave, bumps the group epoch,
     * triggering a recompute that captures a fresh {@link GroupSpec} reflecting whatever offsets have been
     * reported so far — until a captured spec satisfies {@code condition}.
     * In the common case the very first join already sees the offsets and no bouncing happens.
     *
     * @return the first captured spec that satisfies {@code condition}.
     */
    private GroupSpec churnUntilAssignorObserves(
        final StreamsBuilder builder,
        final CapturingTaskAssignor assignor,
        final Predicate<GroupSpec> condition,
        final String timeoutMessage
    ) throws Exception {
        return churnUntilAssignorObserves(builder, assignor, condition, timeoutMessage, Map.of());
    }

    /**
     * As above, but the churn member is started with {@code churnMemberProps}. Passing the {@link #CHURN_TAG} client
     * tag makes the injected assignor hide the churn member and give it an empty assignment, so the real members' task
     * placement stays fixed across the bumps — required by tests that assert a specific stable state (e.g. both
     * standbys caught up), which a normal rebalance would disturb.
     */
    private GroupSpec churnUntilAssignorObserves(
        final StreamsBuilder builder,
        final CapturingTaskAssignor assignor,
        final Predicate<GroupSpec> condition,
        final String timeoutMessage,
        final Map<String, Object> churnMemberProps
    ) throws Exception {
        final long deadlineMs = System.currentTimeMillis() + 120_000L;
        while (System.currentTimeMillis() < deadlineMs) {
            // Join an extra member -> group-epoch bump -> recompute -> the assignor is invoked and captures a spec.
            // Reaching RUNNING means that rebalance (and therefore the capture) has completed.
            final KafkaStreams churnMember =
                startStreams(builder, TestUtils.tempDirectory().getAbsolutePath(), null, churnMemberProps);
            waitForRunning(churnMember);

            final GroupSpec match = firstCapturedSpecMatching(assignor, condition);
            if (match != null) {
                // Leave the extra member running; the surrounding test only needs the original members.
                return match;
            }

            // Not observed yet: leave the group (another epoch bump) so the next iteration rejoins fresh and triggers
            // a new recompute after more offsets have been reported.
            stopChurnMember(churnMember);
        }
        throw new AssertionError(timeoutMessage);
    }

    private static GroupSpec firstCapturedSpecMatching(final CapturingTaskAssignor assignor,
                                                       final Predicate<GroupSpec> condition) {
        for (final GroupSpec spec : assignor.capturedSpecs()) {
            if (condition.test(spec)) {
                return spec;
            }
        }
        return null;
    }

    private void stopChurnMember(final KafkaStreams churnMember) {
        streamsInstances.remove(churnMember);
        churnMember.close(Duration.ofSeconds(30L));
    }

    /**
     * Test assignor that records every {@link GroupSpec} it is asked to assign and delegates the actual assignment
     * to a real {@link StickyTaskAssignor} so the group keeps functioning. Registered under the "sticky" key.
     */
    static final class CapturingTaskAssignor implements TaskAssignor {
        private final StickyTaskAssignor delegate = new StickyTaskAssignor();
        private final List<GroupSpec> capturedSpecs = new CopyOnWriteArrayList<>();
        // Applied to the sticky assignment before returning, so a test can force a specific assignment
        // (e.g. inject a warm-up task). Default: identity (plain sticky).
        private volatile UnaryOperator<GroupAssignment> transform = UnaryOperator.identity();

        void setTransform(final UnaryOperator<GroupAssignment> transform) {
            this.transform = transform;
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public GroupAssignment assign(
            final GroupSpec groupSpec,
            final TopologyDescriber topologyDescriber
        ) throws TaskAssignorException {
            capturedSpecs.add(groupSpec);

            // A "churn" member (marked with the CHURN_TAG client tag) exists only to bump the group epoch so we can
            // observe reported offsets in a fresh capture. Hide it from the delegate so its presence does not move the
            // real members' tasks (sticky sees the same input and returns the same assignment), and hand it an empty
            // assignment. When no churn member is present (all other tests) this is a plain sticky assignment.
            final Map<String, AssignmentMemberSpec> realMembers = new HashMap<>();
            final Set<String> churnMembers = new HashSet<>();
            groupSpec.members().forEach((memberId, member) -> {
                if ("true".equals(member.clientTags().get(CHURN_TAG))) {
                    churnMembers.add(memberId);
                } else {
                    realMembers.put(memberId, member);
                }
            });

            // Apply the test transform (e.g. forcing a warm-up) to the REAL members' assignment only, so it never
            // targets a churn member. Churn members are always assigned empty.
            final GroupAssignment realAssignment = transform.apply(delegate.assign(
                new GroupSpecImpl(realMembers, groupSpec.assignmentConfigs()),
                topologyDescriber
            ));
            if (churnMembers.isEmpty()) {
                return realAssignment;
            }
            final Map<String, MemberAssignment> merged = new HashMap<>(realAssignment.members());
            churnMembers.forEach(memberId -> merged.put(memberId, MemberAssignment.empty()));
            return new GroupAssignment(merged);
        }

        List<GroupSpec> capturedSpecs() {
            return capturedSpecs;
        }
    }

    /**
     * A client supplier whose restore consumer can be paused: while paused, the restore consumer's poll returns no
     * records, so a restoring task makes no progress and stays at its on-disk offset. This holds a task at a
     * controlled lag deterministically (no mock time). Only the restore consumer is overridable here — the main
     * consumer under the streams group protocol is the async consumer, which is not supplied via
     * {@link KafkaClientSupplier} (so processing is paused via {@link KafkaStreams#pause()} instead).
     */
    static final class PausableClientSupplier extends DefaultKafkaClientSupplier {
        private volatile boolean restorePaused;

        void pauseRestore(final boolean paused) {
            this.restorePaused = paused;
        }

        @Override
        public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
            return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()) {
                @Override
                public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
                    return restorePaused ? ConsumerRecords.empty() : super.poll(timeout);
                }
            };
        }
    }

    /**
     * Reflection helpers reaching internal broker/client state. POC-only; remove once the assignor is pluggable.
     */
    private static final class Reflection {

        /**
         * Walks {@code EmbeddedKafkaCluster -> KafkaClusterTestKit.brokers() -> BrokerServer.groupCoordinator() ->
         * GroupCoordinatorService.runtime -> CoordinatorRuntime.coordinators -> CoordinatorContext.coordinator ->
         * SnapshottableCoordinator.coordinator() -> GroupCoordinatorShard.groupMetadataManager} and replaces the
         * "sticky" entry of {@code GroupMetadataManager.streamsGroupAssignors} (a mutable HashMap) with the given
         * assignor. Returns the number of loaded coordinator shards it injected into (expected to be 1 when
         * {@code offsets.topic.num.partitions=1}).
         */
        static void injectStreamsAssignor(final EmbeddedKafkaCluster cluster, final TaskAssignor assignor) throws Exception {
            int injected = 0;
            for (final Object gmm : groupMetadataManagers(cluster)) {
                final Field assignorsField = field(gmm, "streamsGroupAssignors");
                @SuppressWarnings("unchecked")
                final Map<String, TaskAssignor> assignors = (Map<String, TaskAssignor>) assignorsField.get(gmm);
                assignors.put("sticky", assignor);
                injected++;
            }
            if (injected == 0) {
                throw new IllegalStateException("Could not inject streams group assignors.");
            }
        }

        private static List<Object> groupMetadataManagers(final EmbeddedKafkaCluster cluster) throws Exception {
            final Object kit = field(cluster, "cluster").get(cluster);
            final Object brokers = method(kit, "brokers").invoke(kit);
            final List<Object> gmms = new ArrayList<>();
            for (final Object broker : ((Map<?, ?>) brokers).values()) {
                final Object groupCoordinator = method(broker, "groupCoordinator").invoke(broker);
                final Object runtime = field(groupCoordinator, "runtime").get(groupCoordinator);
                final Object coordinators = field(runtime, "coordinators").get(runtime);
                for (final Object context : ((Map<?, ?>) coordinators).values()) {
                    final Object snapshottable = field(context, "coordinator").get(context);
                    if (snapshottable == null) {
                        continue;
                    }
                    final Method coordinatorMethod = method(snapshottable, "coordinator");
                    final Object shard = coordinatorMethod.invoke(snapshottable);
                    gmms.add(field(shard, "groupMetadataManager").get(shard));
                }
            }
            return gmms;
        }

        private static Field field(final Object target, final String name) throws NoSuchFieldException {
            Class<?> clazz = target.getClass();
            while (clazz != null) {
                try {
                    final Field f = clazz.getDeclaredField(name);
                    f.setAccessible(true);
                    return f;
                } catch (final NoSuchFieldException e) {
                    clazz = clazz.getSuperclass();
                }
            }
            throw new NoSuchFieldException(name + " on " + target.getClass());
        }

        private static Method method(final Object target, final String name, final Class<?>... params)
            throws NoSuchMethodException {
            Class<?> clazz = target.getClass();
            while (clazz != null) {
                try {
                    final Method m = clazz.getDeclaredMethod(name, params);
                    m.setAccessible(true);
                    return m;
                } catch (final NoSuchMethodException e) {
                    clazz = clazz.getSuperclass();
                }
            }
            throw new NoSuchMethodException(name + " on " + target.getClass());
        }
    }
}
