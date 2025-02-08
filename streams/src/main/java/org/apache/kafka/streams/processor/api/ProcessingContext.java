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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.StoreBuilder;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Top level context interface for {@link Processor Processors} and {@link FixedKeyProcessor FixedKeyProcessors}.
 *
 * <p>This interface allows to access {@link Topology#connectProcessorAndStateStores(String, String...) connected}
 * {@link StateStore state stores} (also cf. {@link StreamsBuilder#addStateStore(StoreBuilder)}), schedule
 * {@link Punctuator punctuations}, access to topology, runtime, and {@link RecordMetadata record metadata},
 * and to <em>request</emp> offset commits.
 *
 * <p>This interface is not intended to be implemented by user code.
 */
public interface ProcessingContext {

    /**
     * Return the {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_ID_CONFIG application.id}.
     *
     * @return The {@code application.id}.
     */
    String applicationId();

    /**
     * Return the task id.
     *
     * @return The task id.
     */
    TaskId taskId();

    /**
     * Return the {@link RecordMetadata metadata} of the current record if available.
     * Processors may be invoked to process a source record from an input topic, to run
     * {@link ProcessingContext#schedule(Duration, PunctuationType, Punctuator) scheduled punctuation},
     * or because a parent processor called {@link ProcessorContext#forward(Record) forward()}.
     *
     * <p>In the case of a punctuation, there is no source record and this metadata is undefined, and thus not available.
     * Note that when a punctuator invokes {@link ProcessorContext#forward(Record) forward()}, downstream processors
     * will receive the forwarded record as a regular {@link Processor#process(Record)} or
     * {@link FixedKeyProcessor#process(FixedKeyRecord)} invocation.
     * In other words, it wouldn't be apparent to downstream processors whether the record being processed came from an
     * input topic or punctuation and therefore whether this metadata is defined.
     * This is why the return type of this method is {@link Optional}.
     *
     * <p>If there is any possibility of punctuators upstream, any access to this field should consider the case of
     * <code>recordMetadata().isPresent() == false</code>.
     * Of course, it would be safest to always guard this condition.
     *
     * @return The {@link RecordMetadata metadata} of the current record if available.
     */
    Optional<RecordMetadata> recordMetadata();

    /**
     * Return the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG default.key.serde}.
     *
     * @return The default key serde.
     */
    Serde<?> keySerde();

    /**
     * Return the {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG default.value.serde}.
     *
     * @return The default value serde.
     */
    Serde<?> valueSerde();

    /**
     * Return the state directory of the task, i.e, {@code /<state.dir>/<task.id>/}.
     *
     * @return The task's state directory.
     */
    File stateDir();

    /**
     * Return the {@link StreamsMetrics} instance.
     *
     * <p>The {@link StreamsMetrics} instance allows to add custom sensors and record custom metrics.
     *
     * <p><strong>Caution:</strong> Do not remove any metrics added by the Kafka Streams runtime via this interface,
     * as it could lead to runtime exceptions.
     *
     * @return {@link StreamsMetrics} instance.
     */
    StreamsMetrics metrics();

    /**
     * Get a {@link StateStore} given the store name.
     * "Regular" state stores are sharded and a {@link Processor}/{@link FixedKeyProcessor} instance has only access
     * to a single shard of the state store.
     *
     * <p>{@link Topology#addStateStore(StoreBuilder, String...) State stores}
     * (also cf. {@link org.apache.kafka.streams.processor.ConnectedStoreProvider} and
     * {@link StreamsBuilder#addStateStore(StoreBuilder) StreamsBuilder#addStateStore()}) and
     * {@link Topology#addReadOnlyStateStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) read-only state stores}
     * must be {@link Topology#connectProcessorAndStateStores(String, String...) connected} to the processor when the
     * topology is specified, to make them accessible.
     * All {@link Topology#addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier) global state stores}
     * and {@link StreamsBuilder#globalTable(String) global tables} are accessible from any processor automatically.
     *
     * @param name
     *        the state store name
     *
     * @return The state store instance, which needs to be type-casted to the actual {@link StateStore} type.
     */
    <S extends StateStore> S getStateStore(final String name);

    /**
     * Schedule a periodic operation for processors.
     * A processor may call this method during initialization
     * ({@link Processor#init(ProcessorContext) Processor#init()}/{@link FixedKeyProcessor#init(FixedKeyProcessorContext) FixedKeyProcessor#init()},
     * or during processing
     * ({@link Processor#process(Record) Processor#process()}/{@link FixedKeyProcessor#process(FixedKeyRecord) FixedKeyProcessor#process()})
     * to schedule a periodic callback&mdash;called a punctuation&mdash;to {@link Punctuator#punctuate(long)}.
     * The type parameter controls what notion of time is used for punctuation:
     * <ul>
     *   <li>{@link PunctuationType#STREAM_TIME STREAM_TIME}&mdash;use stream-time, which is advanced by the
     *       processing of record in accordance with the timestamp as extracted by the {@link TimestampExtractor} in use.
     *       The first punctuation will be triggered by the first record that is processed.
     *       <b>NOTE:</b> stream-time only advanced if record are processed.</li>
     *   <li>{@link PunctuationType#WALL_CLOCK_TIME WALL_CLOCK_TIME}&mdash;uses system time (also called wall-clock time),
     *       which advances independent of whether new record arrive.
     *       The first punctuation will be triggered after the specified interval has elapsed.</li>
     * </ul>
     * <strong>NOTE:</strong> Punctuation schedules are executed as best effort only, as its granularity is limited by how long
     * an iteration of the internal processing loop takes to complete.
     *
     * <p><strong>Skipping punctuations:</strong> Punctuations will not be triggered more than once at any given
     * timestamp.
     * This means that "missed" punctuation will be skipped.
     * It's possible to "miss" a punctuation if:
     * <ul>
     *   <li>{@link PunctuationType#STREAM_TIME STREAM_TIME}: when stream-time advances (skips ahead) more than the interval</li>
     *   <li>{@link PunctuationType#WALL_CLOCK_TIME WALL_CLOCK_TIME}: on GC pause, too short interval, ...</li>
     * </ul>
     *
     * @param interval
     *        the time interval (cannot be {@code null}) between punctuations (supported minimum is 1 millisecond)
     * @param type
     *        one of: {@link PunctuationType#STREAM_TIME STREAM_TIME}, {@link PunctuationType#WALL_CLOCK_TIME WALL_CLOCK_TIME}; (cannot be {@code null})
     * @param callback
     *        the function (cannot be {@code null}) which will be executed each time a punctuation is triggered
     *
     * @return A handle allowing cancellation of the punctuation schedule established by this method.
     *
     * @throws IllegalArgumentException if the interval is not representable in milliseconds
     */
    Cancellable schedule(final Duration interval,
                         final PunctuationType type,
                         final Punctuator callback);

    /**
     * Request a commit.
     *
     * <p>Note that calling {@code commit()} is only a request for a commit, but it does not execute one.
     * Hence, when {@code commit()} returns, no commit was executed yet.
     * However, Kafka Streams will commit as soon as possible, instead of waiting for next
     * {@link org.apache.kafka.streams.StreamsConfig#COMMIT_INTERVAL_MS_CONFIG commit.interval.ms} to pass.
     */
    void commit();

    /**
     * Return the application {@link org.apache.kafka.streams.StreamsConfig properties} as key/value pairs.
     *
     * <p>The type of the values is dependent on the {@link org.apache.kafka.common.config.ConfigDef.Type type} of the
     * property (e.g., the value of
     * {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG DEFAULT_KEY_SERDE_CLASS_CONFIG}
     * is {@link Class}, even if it was specified as a {@link String} via
     * {@link org.apache.kafka.streams.StreamsConfig#StreamsConfig(Map) StreamsConfig(Map)}).
     *
     * @return All the key/values from the {@link org.apache.kafka.streams.StreamsConfig StreamsConfig} properties.
     */
    Map<String, Object> appConfigs();

    /**
     * Return all the application {@link org.apache.kafka.streams.StreamsConfig} properties
     * which start with the given key prefix, as key/value pairs stripping by the prefix.
     *
     * <p>For example, {@code appConfigsWithPrefix("default.")} will return all the properties that start with
     * {@code default.}, such as {@code default.key.serde}, {@code default.value.serde}, etc.
     * striped by the {@code default.} prefix.
     * If the prefix matches the key fullys, the key/value pair will not be included in the result.
     *
     * @param prefix
     *        the properties prefix used to filter the properties
     *
     * @return All the key/values matching the given prefix from the
     *         {@link org.apache.kafka.streams.StreamsConfig StreamsConfig} properties.
     */
    Map<String, Object> appConfigsWithPrefix(final String prefix);

    /**
     * Return the current system timestamp (also called wall-clock time) in milliseconds.
     *
     * <p> Note: this method returns the internally cached system timestamp from the Kafka Stream runtime.
     * Thus, it may return a different value compared to {@link System#currentTimeMillis()}.
     *
     * <p>It is recommended to use this method instead of {@link System#currentTimeMillis()} to avoid expensive
     * system calls.
     *
     * @return The current system timestamp in milliseconds.
     */
    long currentSystemTimeMs();

    /**
     * Return the current stream-time in milliseconds.
     *
     * <p>Stream-time is the maximum observed {@link TimestampExtractor record timestamp} so far
     * (including the currently processed record), i.e., it can be considered a high-watermark.
     * Stream-time is tracked on a per-task basis and is preserved across restarts and during task migration.
     *
     * <p>Note: this method is not supported for
     * {@link Topology#addGlobalStore(StoreBuilder, String, TimestampExtractor, Deserializer, Deserializer, String, String, ProcessorSupplier) global processors}
     * (also cf. {@link StreamsBuilder#addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier) StreamsBuilder.addGlobalStore(...)}),
     * because there is no concept of stream-time for this case.
     * Calling this method in a global processor will result in an {@link UnsupportedOperationException}.
     *
     * @return The current stream-time in milliseconds.
     *
     * @throws UnsupportedOperationException
     *         if the method is called inside a global processor
     */
    long currentStreamTimeMs();
}