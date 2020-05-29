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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

class Tasks {
    private final Logger log;
    private final InternalTopologyBuilder builder;
    private final StreamsMetricsImpl streamsMetrics;

    private final Map<TaskId, Task> allTasksPerId = new TreeMap<>();
    private final Map<TaskId, Task> readOnlyTasksPerId = Collections.unmodifiableMap(allTasksPerId);
    private final Collection<Task> readOnlyTasks = Collections.unmodifiableCollection(allTasksPerId.values());

    private final Map<TaskId, StreamTask> activeTasksPerId = new TreeMap<>();
    private final Map<TaskId, StreamTask> readOnlyActiveTasksPerId = Collections.unmodifiableMap(activeTasksPerId);
    private final Set<TaskId> readOnlyActiveTaskIds = Collections.unmodifiableSet(activeTasksPerId.keySet());
    private final Collection<StreamTask> readOnlyActiveTasks = Collections.unmodifiableCollection(activeTasksPerId.values());

    private final Map<TaskId, StandbyTask> standbyTasksPerId = new TreeMap<>();
    private final Map<TaskId, StandbyTask> readOnlyStandbyTasksPerId = Collections.unmodifiableMap(standbyTasksPerId);
    private final Set<TaskId> readOnlyStandbyTaskIds = Collections.unmodifiableSet(standbyTasksPerId.keySet());

    // materializing this relationship because the lookup is on the hot path
    private final Map<TopicPartition, StreamTask> activeTasksPerPartition = new HashMap<>();

    private final ActiveTaskCreator activeTaskCreator;
    private final StandbyTaskCreator standbyTaskCreator;

    private Consumer<byte[], byte[]> mainConsumer;

    Tasks(final String logPrefix,
          final InternalTopologyBuilder builder,
          final StreamsMetricsImpl streamsMetrics,
          final ActiveTaskCreator activeTaskCreator,
          final StandbyTaskCreator standbyTaskCreator) {

        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        this.builder = builder;
        this.streamsMetrics = streamsMetrics;
        this.activeTaskCreator = activeTaskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
    }

    void setMainConsumer(final Consumer<byte[], byte[]> mainConsumer) {
        this.mainConsumer = mainConsumer;
    }

    void createTasks(Map<TaskId, Set<TopicPartition>> activeTasksToCreate,
                     Map<TaskId, Set<TopicPartition>> standbyTasksToCreate) {
        for (final Map.Entry<TaskId, Set<TopicPartition>> taskToBeCreated : activeTasksToCreate.entrySet()) {
            final TaskId taskId = taskToBeCreated.getKey();

            if (activeTasksPerId.containsKey(taskId)) {
                throw new IllegalStateException("Attempted to create an active task that we already own: " + taskId);
            }
        }

        for (final Map.Entry<TaskId, Set<TopicPartition>> taskToBeCreated : standbyTasksToCreate.entrySet()) {
            final TaskId taskId = taskToBeCreated.getKey();

            if (standbyTasksPerId.containsKey(taskId)) {
                throw new IllegalStateException("Attempted to create a standby task that we already own: " + taskId);
            }
        }

        // keep this check to simplify testing (ie, no need to mock `activeTaskCreator`)
        if (!activeTasksToCreate.isEmpty()) {
            for (final StreamTask activeTask : activeTaskCreator.createTasks(mainConsumer, activeTasksToCreate)) {
                activeTasksPerId.put(activeTask.id(), activeTask);
                allTasksPerId.put(activeTask.id(), activeTask);
                for (final TopicPartition topicPartition : activeTask.inputPartitions()) {
                    activeTasksPerPartition.put(topicPartition, activeTask);
                }
            }
        }

        // keep this check to simplify testing (ie, no need to mock `standbyTaskCreator`)
        if (!standbyTasksToCreate.isEmpty()) {
            for (final StandbyTask standbyTask : standbyTaskCreator.createTasks(standbyTasksToCreate)) {
                standbyTasksPerId.put(standbyTask.id(), standbyTask);
                allTasksPerId.put(standbyTask.id(), standbyTask);
            }
        }
    }

    void convertActiveToStandby(final StreamTask streamTask, final Set<TopicPartition> partitions) {
        if (activeTasksPerId.remove(streamTask.id()) == null) {
            throw new IllegalStateException("Attempted to convert unknown stream task to standby task: " + streamTask.id());
        }
        final StandbyTask standbyTask = standbyTaskCreator.createStandbyTaskFromActive(streamTask, partitions);
        standbyTasksPerId.put(standbyTask.id(), standbyTask);
        allTasksPerId.put(standbyTask.id(), standbyTask);
    }

    void convertStandbyToActive(final StandbyTask standbyTask, final Set<TopicPartition> partitions) {
        if (standbyTasksPerId.remove(standbyTask.id()) == null) {
            throw new IllegalStateException("Attempted to convert unknown standby task to stream task: " + standbyTask.id());
        }
        final StreamTask activeTask = activeTaskCreator.createActiveTaskFromStandby(standbyTask, partitions, mainConsumer);
        activeTasksPerId.put(activeTask.id(), activeTask);
        allTasksPerId.put(activeTask.id(), activeTask);
    }

    void updateInputPartitionsAndResume(final Task task, final Set<TopicPartition> topicPartitions) {
        final boolean requiresUpdate = !task.inputPartitions().equals(topicPartitions);
        if (requiresUpdate) {
            log.trace("Update task {} inputPartitions: current {}, new {}", task, task.inputPartitions(), topicPartitions);
            for (final TopicPartition inputPartition : task.inputPartitions()) {
                activeTasksPerPartition.remove(inputPartition);
            }
            if (task.isActive()) {
                for (final TopicPartition topicPartition : topicPartitions) {
                    activeTasksPerPartition.put(topicPartition, (StreamTask) task);
                }
            }
            task.update(topicPartitions, builder.buildSubtopology(task.id().topicGroupId));
        }
        task.resume();
    }

    void cleanUpTaskProducerAndRemoveTask(final TaskId taskId,
                                          final Map<TaskId, RuntimeException> taskCloseExceptions) {
        try {
            activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId);
        } catch (final RuntimeException e) {
            final String uncleanMessage = String.format("Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:", taskId);
            log.error(uncleanMessage, e);
            taskCloseExceptions.putIfAbsent(taskId, e);
        }
        activeTasksPerId.remove(taskId);
        standbyTasksPerId.remove(taskId);
        allTasksPerId.remove(taskId);
    }

    void closeAndRemoveTaskProducerIfNeeded(final StreamTask activeTask) {
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(activeTask.id());
    }

    // Note: this MUST be called *before* actually closing the task
    void cleanupTask(final Task task) {
        for (final TopicPartition inputPartition : task.inputPartitions()) {
            activeTasksPerPartition.remove(inputPartition);
        }
        final String threadId = Thread.currentThread().getName();
        streamsMetrics.removeAllTaskLevelSensors(threadId, task.id().toString());
    }

    void reInitializeThreadProducer() {
        activeTaskCreator.reInitializeThreadProducer();
    }

    void closeThreadProducerIfNeeded() {
        activeTaskCreator.closeThreadProducerIfNeeded();
    }

    void clear() {
        activeTasksPerId.clear();
        standbyTasksPerId.clear();
        allTasksPerId.clear();
    }

    StreamTask activeTasksForInputPartition(final TopicPartition partition) {
        return activeTasksPerPartition.get(partition);
    }

    StandbyTask standbyTask(final TaskId taskId) {
        if (!standbyTasksPerId.containsKey(taskId)) {
            throw new IllegalStateException("Standby task unknown: " + taskId);
        }
        return standbyTasksPerId.get(taskId);
    }

    Task task(final TaskId taskId) {
        if (!allTasksPerId.containsKey(taskId)) {
            throw new IllegalStateException("Task unknown: " + taskId);
        }
        return allTasksPerId.get(taskId);
    }

    Collection<StreamTask> activeTasks() {
        return readOnlyActiveTasks;
    }

    Collection<Task> allTasks() {
        return readOnlyTasks;
    }

    Set<TaskId> activeTaskIds() {
        return readOnlyActiveTaskIds;
    }

    Set<TaskId> standbyTaskIds() {
        return readOnlyStandbyTaskIds;
    }

    Map<TaskId, StreamTask> activeTaskMap() {
        return readOnlyActiveTasksPerId;
    }

    Map<TaskId, StandbyTask> standbyTaskMap() {
        return readOnlyStandbyTasksPerId;
    }

    Map<TaskId, Task> tasksPerId() {
        return readOnlyTasksPerId;
    }

    boolean owned(final TaskId taskId) {
        return allTasksPerId.containsKey(taskId);
    }

    StreamsProducer streamsProducerForTask(final TaskId taskId) {
        return activeTaskCreator.streamsProducerForTask(taskId);
    }

    StreamsProducer threadProducer() {
        return activeTaskCreator.threadProducer();
    }

    Map<MetricName, Metric> producerMetrics() {
        return activeTaskCreator.producerMetrics();
    }

    Set<String> producerClientIds() {
        return activeTaskCreator.producerClientIds();
    }

    // for testing only
    void addTask(final TaskId taskId, final Task task) {
        if (task.isActive()) {
            activeTasksPerId.put(taskId, (StreamTask) task);
        } else {
            standbyTasksPerId.put(taskId, (StandbyTask) task);
        }
        allTasksPerId.put(taskId, task);
    }

}
