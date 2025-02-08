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
package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

/**
 * Allows to implicitly {@link Topology#addStateStore(StoreBuilder, String...) add} {@link StateStore state stores} to
 * a {@link Topology} and automatically {@link Topology#connectProcessorAndStateStores(String, String...) connect} them
 * to the corresponding {@link Topology#addProcessor(String, ProcessorSupplier, String...) processor}.
 *
 * <p>Using this interface is recommended when the associated processor wants to encapsulate its usage of its
 * {@link StateStore state stores}, rather than exposing them to the user building the topology.
 * If different processors want to share the same {@link StateStore}, this interface should not be used, but
 * {@link StateStore state stores} should be {@link Topology#addStateStore(StoreBuilder, String...) added expliclity}
 * to the {@link Topology}
 * (also cf. {@link org.apache.kafka.streams.StreamsBuilder#addStateStore(StoreBuilder) StreamsBuilder#addStateStore(...)}).
 *
 * <p>When a {@link Topology#addProcessor(String, ProcessorSupplier, String...) processor} is added to a topology,
 * and the {@link ProcessorSupplier} (or {@link FixedKeyProcessorSupplier}) overwrites the {@link #stores()} method,
 * for each returned {@link StoreBuilder}, a corresponding {@link StateStore} will be added to the topology with
 * store name {@link StoreBuilder#name()}, and connected to the processor:
 *
 * <pre>{@code
 * public class MyProcessorSupplier implements ProcessorSupplier<String, Integer, String, Integer>, ConnectedStoreProvider {
 *
 *   @Override
 *   Processor<KIn, VIn, KOut, VOut> get() {
 *     return new Processor<>() {
 *       private KeyValueStore<String, String> store;
 *
 *       @Override
 *       public void init(final ProcessorContext<String, Integer> context) {
 *         // Processor can access the provided store
 *         store = context.getStateStore("myStore");
 *        }
 *
 *       @Override
 *       public void process(final Record<String, Integer> record) {
 *         ...
 *       }
 *     };
 *   }
 *
 *   @Override
 *   public Set<StoreBuilder<?>> stores() {
 *     return Collections.singleton(
 *       Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("myStore"), Serdes.String(), Serdes.String())
 *     );
 *   }
 * }</pre>
 */
public interface ConnectedStoreProvider {

    /**
     * Return a set of {@link StoreBuilder StoreBuilders}.
     * The that will be automatically added to the {@link Topology} and connected to the associated
     * {@link Topology#addProcessor(String, ProcessorSupplier, String...) processor}.
     *
     * @return The state stores for a {@link org.apache.kafka.streams.processor.api.Processor}
     */
    default Set<StoreBuilder<?>> stores() {
        return null;
    }
}