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
 * A processor of key-value pair {@link Record records} where the key is immutable.
 * If you need to modify the key, use {@link Processor} instead.
 *
 * <p>{@link FixedKeyProcessor FixedKeyProcessors} are created by
 * {@link FixedKeyProcessorSupplier FixedKeyProcessorSuppliers} which are
 * {@link org.apache.kafka.streams.kstream.KStream#processValues(FixedKeyProcessorSupplier, String...) added} to a
 * Kafka Streams DSL program.
 *
 * <p>This interface is not intended to be implemented by user code.

 * @param <KIn> the type of input keys
 * @param <VIn> the type of input values
 * @param <VOut> the type of output values
 *
 * @see ContextualFixedKeyProcessor
 */
public interface FixedKeyProcessor<KIn, VIn, VOut> {

    /**
     * Initialize this processor with the given context.
     * Kafka Streams ensures this method is called once per processor instance during topology initialization.
     * When Kafka Streams is done with the processor, {@link #close()} will be called.
     * Kafka Streams may later re-use the processor by calling {@code #init()} again.
     *
     * <p>The provided context is mainly used to {@link FixedKeyProcessorContext#forward(FixedKeyRecord) forward}
     * output records within {@link #process(FixedKeyRecord) process()}, and provides access to
     * {@link StateStore StateStores} and metadata.
     *
     * <p>Note that calling {@link FixedKeyProcessorContext#forward(FixedKeyRecord) forward()} from a
     * {@link Punctuator#punctuate(long) Punctuator} is not supported, as it cannot guarantee that a "correct" key is
     * set on the output record.
     *
     * @param context the {@link FixedKeyProcessorContext context} for this processor
     */
    default void init(final FixedKeyProcessorContext<KIn, VOut> context) {}

    /**
     * Process a record.
     *
     * <p>Note that {@link FixedKeyProcessorContext#recordMetadata() record metadata} is undefined in cases such as a
     * {@link FixedKeyProcessorContext#forward(FixedKeyRecord) forward()} call from a
     * {@link Punctuator#punctuate(long) punctuator}.
     *
     * @param record the record to process
     */
    void process(FixedKeyRecord<KIn, VIn> record);

    /**
     * Close this processor and clean up any resources.
     * Be aware that {@code #close()} is called after an internal cleanup. Thus, it is not possible to write anything
     * to Kafka as underlying clients are already closed.
     * Kafka Streams may later re-use this processor by calling {@code #init()} again.
     *
     * <p>Note: Do not close any managed resources like {@link StateStore StateStores}.
     */
    default void close() {}
}