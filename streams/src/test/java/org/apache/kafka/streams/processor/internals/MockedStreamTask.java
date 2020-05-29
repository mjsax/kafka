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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.InternalMockProcessorContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.niceMock;

class MockedStreamTask extends StreamTask {
    private boolean commitNeeded = false;
    private boolean commitRequested = false;
    private boolean commitPrepared = false;
    private Map<TopicPartition, OffsetAndMetadata> committableOffsets = Collections.emptyMap();
    private Map<TopicPartition, Long> purgeableOffsets;
    private Map<TopicPartition, Long> changelogOffsets = Collections.emptyMap();

    private final Map<TopicPartition, LinkedList<ConsumerRecord<byte[], byte[]>>> queue = new HashMap<>();

    MockedStreamTask(final TaskId id,
                     final Set<TopicPartition> partitions) {
        this(id, partitions, niceMock(ProcessorStateManager.class));
    }

    MockedStreamTask(final TaskId id,
                     final Set<TopicPartition> partitions,
                     final ProcessorStateManager processorStateManager) {
        super(
            id,
            partitions,
            new ProcessorTopology(
                Collections.emptyList(),
                partitions.stream().collect(Collectors.toMap(TopicPartition::topic, p -> niceMock(SourceNode.class))),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptySet()
            ),
            null,
            new StreamsConfig(mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"dummy:1234")
            )),
            new MockStreamsMetrics(new Metrics()),
            null,
            null,
            null,
            processorStateManager,
            null,
            new InternalMockProcessorContext()
        );
    }

    @Override
    public void initializeIfNeeded() {
        if (state() == State.CREATED) {
            transitionTo(State.RESTORING);
        }
    }

    public void completeRestoration() {
        transitionTo(State.RUNNING);
    }

    public void setCommitNeeded() {
        commitNeeded = true;
    }

    @Override
    public boolean commitNeeded() {
        return commitNeeded;
    }

    public void setCommitRequested() {
        commitRequested = true;
    }

    @Override
    public boolean commitRequested() {
        return commitRequested;
    }

    @Override
    public void prepareCommit() {
        commitPrepared = true;
    }

    public boolean commitPrepared() {
        return commitPrepared;
    }

    @Override
    public void postCommit() {
        commitNeeded = false;
    }

    @Override
    public void prepareSuspend() {}

    @Override
    public void suspend() {
        transitionTo(State.SUSPENDED);
    }

    @Override
    public void resume() {
        if (state() == State.SUSPENDED) {
            transitionTo(State.RUNNING);
        }
    }

    @Override
    public Map<TopicPartition, Long> prepareCloseClean() {
        return Collections.emptyMap();
    }

    @Override
    public void prepareCloseDirty() {}

    @Override
    public void closeClean(final Map<TopicPartition, Long> checkpoint) {
        transitionTo(State.CLOSED);
    }

    @Override
    public void closeDirty() {
        transitionTo(State.CLOSED);
    }

    @Override
    public void closeAndRecycleState() {
        transitionTo(State.CLOSED);
    }

    void setCommittableOffsetsAndMetadata(final Map<TopicPartition, OffsetAndMetadata> committableOffsets) {
        this.committableOffsets = committableOffsets;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committableOffsetsAndMetadata() {
        return committableOffsets;
    }

    @Override
    public StateStore getStore(final String name) {
        return null;
    }

    @Override
    public Collection<TopicPartition> changelogPartitions() {
        return changelogOffsets.keySet();
    }

    public boolean isActive() {
        return true;
    }

    void setPurgeableOffsets(final Map<TopicPartition, Long> purgeableOffsets) {
        this.purgeableOffsets = purgeableOffsets;
    }

    @Override
    public Map<TopicPartition, Long> purgeableOffsets() {
        return purgeableOffsets;
    }

    void setChangelogOffsets(final Map<TopicPartition, Long> changelogOffsets) {
        this.changelogOffsets = changelogOffsets;
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return changelogOffsets;
    }

    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        if (isActive()) {
            final Deque<ConsumerRecord<byte[], byte[]>> partitionQueue =
                queue.computeIfAbsent(partition, k -> new LinkedList<>());

            for (final ConsumerRecord<byte[], byte[]> record : records) {
                partitionQueue.add(record);
            }
        } else {
            throw new IllegalStateException("Can't add records to an inactive task.");
        }
    }

    @Override
    public boolean process(final long wallClockTime) {
        if (isActive() && state() == State.RUNNING) {
            for (final LinkedList<ConsumerRecord<byte[], byte[]>> records : queue.values()) {
                final ConsumerRecord<byte[], byte[]> record = records.poll();
                if (record != null) {
                    return true;
                }
            }
            return false;
        } else {
            throw new IllegalStateException("Can't process an inactive or non-running task.");
        }
    }
}
