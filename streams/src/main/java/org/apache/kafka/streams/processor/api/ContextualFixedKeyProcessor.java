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

/**
 * An abstract implementation of {@link FixedKeyProcessor} that manages the {@link FixedKeyProcessorContext} instance.

 * <p>Using this interface instead of {@link FixedKeyProcessor} may avoid undesired boilerplate code:
 * <pre>{@code
 * public class MyProcessor implements ContextualFixedKeyProcessor<String String, Integer> {
 *   @Override
 *   public void process(final FixedKeyRecord<String, String> record) {
 *     // use FixedKeyProcessorContext w/o the need to overwrite `init(FixedKeyProcessorContext<KIn, VOut>)` method
 *     context().forward(...);
 *   }
 * }
 * }</pre>

 * @param <KIn> the input record key type
 * @param <VIn> the input record value type
 * @param <VOut> the output record key type
 */
public abstract class ContextualFixedKeyProcessor<KIn, VIn, VOut> implements FixedKeyProcessor<KIn, VIn, VOut> {

    private FixedKeyProcessorContext<KIn, VOut> context;

    protected ContextualFixedKeyProcessor() {}

    @Override
    public void init(final FixedKeyProcessorContext<KIn, VOut> context) {
        this.context = context;
    }

    /**
     * Get the processor's {@link FixedKeyProcessorContext context} set during
     * {@link #init(FixedKeyProcessorContext) initialization}.
     *
     * @return The processor's {@link FixedKeyProcessorContext context}.
     *         {@code null} if called prior to {@link #init(FixedKeyProcessorContext) initialization}.
     */
    protected final FixedKeyProcessorContext<KIn, VOut> context() {
        return context;
    }
}