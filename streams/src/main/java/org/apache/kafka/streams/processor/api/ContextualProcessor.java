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
 * An abstract implementation of {@link Processor} that manages the {@link ProcessorContext} instance.
 *
 * <p>Using this interface instead of {@link Processor} may avoid undesired boilerplate code:
 * <pre>{@code
 * public class MyProcessor implements ContextualProcessor<String String, Integer, Integer> {
 *   @Override
 *   public void process(final Record<KIn, VIn> record) {
 *     // use ProcessorContext w/o the need to overwrite `init(ProcessorContext<KOut, VOut>)` method
 *     context().forward(...);
 *   }
 * }
 * }</pre>
 *
 * @param <KIn> the input record key type
 * @param <VIn> the input record value type
 * @param <KOut> the output record key type
 * @param <VOut> the output record value type
 */
public abstract class ContextualProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private ProcessorContext<KOut, VOut> context;

    protected ContextualProcessor() {}

    @Override
    public void init(final ProcessorContext<KOut, VOut> context) {
        this.context = context;
    }

    /**
     * Get the processor's {@link ProcessorContext context} set during {@link #init(ProcessorContext) initialization}.
     *
     * @return The processor's {@link ProcessorContext context}.
     *         {@code null} if called prior to {@link #init(ProcessorContext) initialization}.
     */
    protected final ProcessorContext<KOut, VOut> context() {
        return context;
    }
}