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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.niceMock;

class MockedStandbyTask extends StandbyTask {
    private boolean commitNeeded = false;
    private boolean commitPrepared = false;
    private Map<TopicPartition, Long> changelogOffsets = Collections.emptyMap();

    private final Map<TopicPartition, LinkedList<ConsumerRecord<byte[], byte[]>>> queue = new HashMap<>();

    MockedStandbyTask(final TaskId id,
                      final Set<TopicPartition> partitions) {
        this(id, partitions, null);
    }

    MockedStandbyTask(final TaskId id,
                      final Set<TopicPartition> partitions,
                      final ProcessorStateManager processorStateManager) {
        super(
            id,
            partitions,
            null,
            new StreamsConfig(mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"dummy:1234")
            )),
            new MockStreamsMetrics(new Metrics()),
            null,
            null,
            null,
            niceMock(InternalProcessorContext.class)
        );
    }

    @Override
    public void initializeIfNeeded() {
        if (state() == State.CREATED) {
            transitionTo(State.RESTORING);
            transitionTo(State.RUNNING);
        }
    }

    public void setCommitNeeded() {
        commitNeeded = true;
    }

    @Override
    public boolean commitNeeded() {
        return commitNeeded;
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

    @Override
    public StateStore getStore(final String name) {
        return null;
    }

    @Override
    public Collection<TopicPartition> changelogPartitions() {
        return changelogOffsets.keySet();
    }

    public boolean isActive() {
        return false;
    }

    void setChangelogOffsets(final Map<TopicPartition, Long> changelogOffsets) {
        this.changelogOffsets = changelogOffsets;
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return changelogOffsets;
    }

}
