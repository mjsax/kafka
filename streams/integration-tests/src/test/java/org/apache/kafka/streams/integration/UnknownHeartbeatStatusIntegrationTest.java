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
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asserts how a Kafka Streams client ought to behave when the group coordinator returns a heartbeat status code the
 * client does not recognize, and therefore <strong>fails against the current client, which swallows the error</strong>.
 *
 * <p>A status code stands for a condition the client is expected to react to. A new code is a semantic contract change
 * -- it requires a version bump, and the broker must withhold it from clients that predate it -- so a code the client
 * cannot interpret means that contract was violated, which the client cannot honour. Whatever the client does with it,
 * the one thing it must not do is continue silently: the requirement is that the error reaches the application, naming
 * the code it could not interpret.
 *
 * <p>The danger is not just a lost status but a lost <em>assignment</em>. The client decodes the status inside the
 * heartbeat-response callback, before it applies the assignment carried on the same response; on the current client the
 * decode of an unknown code throws there and is discarded, so the client keeps heartbeating and silently drops both the
 * status and the assignment. To exercise exactly that, a single client joins cleanly with two stream threads -- one
 * task each -- and only then is the coordinator made to return an unknown code, on the next heartbeat that delivers a
 * changed assignment. Removing one thread frees its task; the surviving thread must pick it up, but the heartbeat that
 * hands it over also carries the unknown code, so a client that swallows the decode never applies the addition. The
 * freed task then runs nowhere while the client stays RUNNING and raises nothing -- an input partition silently stops
 * being consumed.
 *
 * <p>The probe deliberately skips the joining heartbeat (a joining member re-sends its topology, and a swallowed decode
 * that strands it in JOINING at an advanced epoch draws an unrelated "topology at a non-zero epoch" rejection that
 * would mask the swallow) and fires only on an assignment-carrying heartbeat. This test is therefore expected to fail
 * until the client rejects an unknown status code and surfaces it (KAFKA-20981).
 */
@Timeout(600)
@Tag("integration")
public class UnknownHeartbeatStatusIntegrationTest {

    private static final int NUM_PARTITIONS = 2;
    private static final int NUM_RECORDS = 100;
    private static final Set<String> ALL_TASKS = Set.of("0_0", "0_1");
    private static final Set<Integer> PROCESSED_KEYS = ConcurrentHashMap.newKeySet();

    private static EmbeddedKafkaCluster cluster;
    private static String bootstrapServers;

    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties brokerProps = new Properties();
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, "200");
        // Recompute the assignment on every heartbeat rather than once per interval, so the reassignment triggered by
        // removing a thread reaches the surviving member promptly (the default 1s interval would otherwise delay it).
        brokerProps.put(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, "0");
        cluster = new EmbeddedKafkaCluster(1, brokerProps);
        cluster.start();
        bootstrapServers = cluster.bootstrapServers();
    }

    @AfterAll
    public static void stopCluster() {
        System.clearProperty("kafka20860.injectUnknownStatus");
        cluster.stop();
        cluster = null;
    }

    @Test
    public void shouldSurfaceAnUnknownStatusCodeInsteadOfSilentlyDroppingAReassignedTask() throws Exception {
        final String appId = "unknown-heartbeat-status-app";
        final String inputTopic = appId + "-input";
        cluster.createTopic(inputTopic, NUM_PARTITIONS, 1);
        cluster.setGroupStreamsInitialRebalanceDelay(appId, 0);

        produceInputRecords(inputTopic);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .foreach((key, value) -> PROCESSED_KEYS.add(key));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig(appId));
        final AtomicReference<Throwable> uncaught = new AtomicReference<>();
        streams.setUncaughtExceptionHandler(error -> {
            uncaught.compareAndSet(null, error);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        try (final Admin admin = createAdmin()) {
            // Join cleanly with two threads, one task each, so both input partitions are being consumed. No unknown
            // status is injected yet -- the probe only fires on a later assignment-carrying heartbeat.
            streams.start();
            TestUtils.waitForCondition(
                () -> runningTaskIds(streams).equals(ALL_TASKS),
                60_000L,
                "Expected the client to run both tasks (one per thread) before the unknown status is injected");

            // Arm the probe, then free one task by removing its thread. The surviving thread must take the freed task
            // over, but the heartbeat that hands it over now also carries the unknown status.
            System.setProperty("kafka20860.injectUnknownStatus", "true");
            streams.removeStreamThread();

            TestUtils.waitForCondition(
                () -> {
                    final KafkaStreams.State state = streams.state();
                    final Throwable error = uncaught.get();
                    final GroupState groupState = groupState(admin, appId);
                    final Set<String> running = runningTaskIds(streams);

                    // The error the client raises must name the status code it could not interpret. Without the fix it
                    // raises nothing: it swallows the decode, never applies the handover, and stays RUNNING with the
                    // freed task running nowhere -- so the running set is missing a task the group still believes is
                    // assigned. Requiring the code in the message is what separates surfacing it from silently dropping
                    // the reassigned task.
                    final boolean shouldSurfaceTheUnknownStatus = errorChain(error).contains("99");
                    if (!shouldSurfaceTheUnknownStatus) {
                        System.out.println("[unmet] The client should fail with an error naming the unknown status "
                            + "code, but state is " + state + " (group " + groupState + "), it runs " + running
                            + " of " + ALL_TASKS + ", and the surfaced error is "
                            + (error == null ? "none" : errorChain(error)));
                    }

                    return shouldSurfaceTheUnknownStatus;
                },
                30_000L,
                3_000L,
                () -> "A client that receives an unknown status code on the heartbeat that hands it a reassigned task "
                    + "should surface the error instead of silently dropping the task"
            );
        } finally {
            streams.close(Duration.ofSeconds(60));
            PROCESSED_KEYS.clear();
            System.clearProperty("kafka20860.injectUnknownStatus");
        }
    }

    /**
     * The tasks the client actually runs, taken from the local thread metadata, so a task that was assigned but never
     * started does not appear here.
     */
    private static Set<String> runningTaskIds(final KafkaStreams streams) {
        final Set<String> taskIds = new TreeSet<>();
        for (final ThreadMetadata threadMetadata : streams.metadataForLocalThreads()) {
            for (final TaskMetadata taskMetadata : threadMetadata.activeTasks()) {
                taskIds.add(taskMetadata.taskId().toString());
            }
        }
        return taskIds;
    }

    private static String errorChain(final Throwable error) {
        final StringBuilder sb = new StringBuilder();
        for (Throwable t = error; t != null && t != t.getCause(); t = t.getCause()) {
            sb.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" | ");
        }
        return sb.toString();
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

    private static void produceInputRecords(final String inputTopic) throws Exception {
        final List<KeyValue<Integer, Integer>> records = new ArrayList<>(NUM_RECORDS);
        for (int key = 0; key < NUM_RECORDS; key++) {
            records.add(KeyValue.pair(key, key));
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic,
            records,
            TestUtils.producerConfig(bootstrapServers, IntegerSerializer.class, IntegerSerializer.class),
            cluster.time
        );
    }

    private static Properties streamsConfig(final String appId) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        config.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        return config;
    }
}
