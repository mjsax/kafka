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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.ValueAndTimestamp;

class ForwardingCacheFlushListener<K, V> implements CacheFlushListener<K, ValueAndTimestamp<V>> {
    private final InternalProcessorContext context;
    private final boolean sendOldValues;
    private final ProcessorNode myNode;

    ForwardingCacheFlushListener(final ProcessorContext context, final boolean sendOldValues) {
        this.context = (InternalProcessorContext) context;
        myNode = this.context.currentNode();
        this.sendOldValues = sendOldValues;
    }

    @Override
    public void apply(final K key, final ValueAndTimestamp<V> newValue, final ValueAndTimestamp<V> oldValue) {
        final ProcessorNode prev = context.currentNode();
        context.setCurrentNode(myNode);
        try {
            if (sendOldValues) {
                context.forward(
                    key,
                    new Change<>(
                        newValue == null ? null : newValue.value(),
                        oldValue == null ? null : oldValue.value()));
            } else {
                context.forward(key, new Change<>(newValue == null ? null : newValue.value(), null));
            }
        } finally {
            context.setCurrentNode(prev);
        }
    }
}
