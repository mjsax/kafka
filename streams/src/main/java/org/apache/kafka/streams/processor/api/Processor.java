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

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;

import java.time.Duration;

/**
 * A processor of key-value pair {@link Record records}.
 *
 * <p>{@link Processor Processors} are created by {@link ProcessorSupplier ProcessorSuppliers} which are
 * {@link org.apache.kafka.streams.Topology#addProcessor(String, ProcessorSupplier, String...) added} to a
 * {@link org.apache.kafka.streams.Topology Topology}.
 *
 * @param <KIn> the input record key type
 * @param <VIn> the input record value type
 * @param <KOut> the output record key type
 * @param <VOut> the output record key type
 *
 * @see ContextualProcessor
 * @see FixedKeyProcessor
 * @see org.apache.kafka.streams.kstream.KStream#process(ProcessorSupplier, String...) KStream#process(...)
 */
public interface Processor<KIn, VIn, KOut, VOut> {

    /**
     * Initialize this processor with the given context.
     * Kafka Streams ensures this method is called once per processor instance during topology initialization.
     * When Kafka Streams is done with the processor, {@link #close()} will be called.
     * Kafka Streams may later re-use the processor by calling {@code #init()} again.
     *
     * <p>The provided context is mainly used to {@link ProcessorContext#forward(Record) forward} output records within
     * {@link #process(Record) process()} or {@link Punctuator#punctuate(long) punctuations}, and provides access
     * to {@link StateStore StateStores} and metadata.
     *
     * @param context the {@link ProcessorContext context} for this processor
     */
    default void init(final ProcessorContext<KOut, VOut> context) {}

    /**
     * Process a record.
     *
     * <p>Note that {@link ProcessorContext#recordMetadata() record metadata} is undefined in cases such as a
     * {@link ProcessorContext#forward(Record) forward()} call from a {@link Punctuator#punctuate(long) punctuator}.
     *
     * @param record the record to process
     */
    void process(Record<KIn, VIn> record);

    /**
     * Close this processor and clean up any resources.
     * Be aware that {@code #close()} is called after an internal cleanup.
     * Thus, it is not possible to write anything to Kafka as underlying clients are already closed.
     * Kafka Streams may later re-use this processor by calling {@code #init()} again.
     *
     * <p>Note: Do not close any managed resources like {@link StateStore StateStores}.
     */
    default void close() {}
}