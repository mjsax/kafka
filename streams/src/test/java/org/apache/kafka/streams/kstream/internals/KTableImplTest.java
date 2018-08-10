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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.SinkNode;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.ValueAndTimestampImpl;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KTableImplTest {

    private final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
    private final Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.String(), Serdes.String());
    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    private KTable<String, String> table;

    @Before
    public void setUp() {
        final StreamsBuilder builder = new StreamsBuilder();
        table = builder.table("test");
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTable<String, String> table1 = builder.table(topic1, consumed);

        final MockProcessorSupplier<String, Object> supplier = new MockProcessorSupplier<>();
        table1.toStream().process(supplier);

        final KTable<String, Integer> table2 = table1.mapValues(Integer::new);

        table2.toStream().process(supplier);

        final KTable<String, Integer> table3 = table2.filter((key, value) -> (value % 2) == 0);

        table3.toStream().process(supplier);

        table1.toStream().to(topic2, produced);
        final KTable<String, String> table4 = builder.table(topic2, consumed);

        table4.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "01"));
            driver.pipeInput(recordFactory.create(topic1, "B", "02"));
            driver.pipeInput(recordFactory.create(topic1, "C", "03"));
            driver.pipeInput(recordFactory.create(topic1, "D", "04"));
        }

        final List<MockProcessor<String, Object>> processors = supplier.capturedProcessors(4);
        assertEquals(Utils.mkList("A:01", "B:02", "C:03", "D:04"), processors.get(0).processed);
        assertEquals(Utils.mkList("A:1", "B:2", "C:3", "D:4"), processors.get(1).processed);
        assertEquals(Utils.mkList("A:null", "B:2", "C:null", "D:4"), processors.get(2).processed);
        assertEquals(Utils.mkList("A:01", "B:02", "C:03", "D:04"), processors.get(3).processed);
    }

    @Test
    public void testValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 =
            (KTableImpl<String, String, Integer>) table1.mapValues(Integer::new);
        final KTableImpl<String, Integer, Integer> table3 =
            (KTableImpl<String, Integer, Integer>) table2.filter((key, value) -> (value % 2) == 0);

        table1.toStream().to(topic2, produced);
        final KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        final Topology topology = builder.build();

        final KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();
        final KTableValueGetterSupplier<String, String> getterSupplier4 = table4.valueGetterSupplier();

        final InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        topologyBuilder.connectProcessorAndStateStores(table1.name, getterSupplier1.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table2.name, getterSupplier2.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table3.name, getterSupplier3.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table4.name, getterSupplier4.storeNames());

        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(topology, props)) {

            assertEquals(2, driver.getAllStateStores().size());

            final KTableValueGetter<String, String> getter1 = getterSupplier1.get();
            final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();
            final KTableValueGetter<String, String> getter4 = getterSupplier4.get();

            getter1.init(driver.setCurrentNodeForProcessorContext(table1.name));
            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));
            getter4.init(driver.setCurrentNodeForProcessorContext(table4.name));

            driver.pipeInput(recordFactory.create(topic1, "A", "01", 1L));
            driver.pipeInput(recordFactory.create(topic1, "B", "01", 2L));
            driver.pipeInput(recordFactory.create(topic1, "C", "01", 3L));

            assertEquals(new ValueAndTimestampImpl<>("01", 1L), getter1.get("A"));
            assertEquals(new ValueAndTimestampImpl<>("01", 2L), getter1.get("B"));
            assertEquals(new ValueAndTimestampImpl<>("01", 3L), getter1.get("C"));

            assertEquals(new ValueAndTimestampImpl<>(1, 1L), getter2.get("A"));
            assertEquals(new ValueAndTimestampImpl<>(1, 2L), getter2.get("B"));
            assertEquals(new ValueAndTimestampImpl<>(1, 3L), getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertNull(getter3.get("C"));

            assertEquals(new ValueAndTimestampImpl<>("01", 1L), getter4.get("A"));
            assertEquals(new ValueAndTimestampImpl<>("01", 2L), getter4.get("B"));
            assertEquals(new ValueAndTimestampImpl<>("01", 3L), getter4.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", "02", 10L));
            driver.pipeInput(recordFactory.create(topic1, "B", "02", 11L));

            assertEquals(new ValueAndTimestampImpl<>("02", 10L), getter1.get("A"));
            assertEquals(new ValueAndTimestampImpl<>("02", 11L), getter1.get("B"));
            assertEquals(new ValueAndTimestampImpl<>("01", 3L), getter1.get("C"));

            assertEquals(new ValueAndTimestampImpl<>(2, 10L), getter2.get("A"));
            assertEquals(new ValueAndTimestampImpl<>(2, 11L), getter2.get("B"));
            assertEquals(new ValueAndTimestampImpl<>(1, 3L), getter2.get("C"));

            assertEquals(new ValueAndTimestampImpl<>(2, 10L), getter3.get("A"));
            assertEquals(new ValueAndTimestampImpl<>(2, 11L), getter3.get("B"));
            assertNull(getter3.get("C"));

            assertEquals(new ValueAndTimestampImpl<>("02", 10L), getter4.get("A"));
            assertEquals(new ValueAndTimestampImpl<>("02", 11L), getter4.get("B"));
            assertEquals(new ValueAndTimestampImpl<>("01", 3L), getter4.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", "03", 5L));

            assertEquals(new ValueAndTimestampImpl<>("03", 5L), getter1.get("A"));
            assertEquals(new ValueAndTimestampImpl<>("02", 11L), getter1.get("B"));
            assertEquals(new ValueAndTimestampImpl<>("01", 3L), getter1.get("C"));

            assertEquals(new ValueAndTimestampImpl<>(3, 5L), getter2.get("A"));
            assertEquals(new ValueAndTimestampImpl<>(2, 11L), getter2.get("B"));
            assertEquals(new ValueAndTimestampImpl<>(1, 3L), getter2.get("C"));

            assertNull(getter3.get("A"));
            assertEquals(new ValueAndTimestampImpl<>(2, 11L), getter3.get("B"));
            assertNull(getter3.get("C"));

            assertEquals(new ValueAndTimestampImpl<>("03", 5L), getter4.get("A"));
            assertEquals(new ValueAndTimestampImpl<>("02", 11L), getter4.get("B"));
            assertEquals(new ValueAndTimestampImpl<>("01", 3L), getter4.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null));

            assertNull(getter1.get("A"));
            assertEquals(new ValueAndTimestampImpl<>("02", 11L), getter1.get("B"));
            assertEquals(new ValueAndTimestampImpl<>("01", 3L), getter1.get("C"));


            assertNull(getter2.get("A"));
            assertEquals(new ValueAndTimestampImpl<>(2, 11L), getter2.get("B"));
            assertEquals(new ValueAndTimestampImpl<>(1, 3L), getter2.get("C"));

            assertNull(getter3.get("A"));
            assertEquals(new ValueAndTimestampImpl<>(2, 11L), getter3.get("B"));
            assertNull(getter3.get("C"));

            assertNull(getter4.get("A"));
            assertEquals(new ValueAndTimestampImpl<>("02", 11L), getter4.get("B"));
            assertEquals(new ValueAndTimestampImpl<>("01", 3L), getter4.get("C"));
        }
    }

    @Test
    public void testStateStoreLazyEval() {
        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final StreamsBuilder builder = new StreamsBuilder();

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        builder.table(topic2, consumed);

        final KTableImpl<String, String, Integer> table1Mapped =
            (KTableImpl<String, String, Integer>) table1.mapValues(Integer::new);
        table1Mapped.filter((key, value) -> (value % 2) == 0);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertEquals(2, driver.getAllStateStores().size());
        }
    }

    @Test
    public void testStateStore() {
        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final StreamsBuilder builder = new StreamsBuilder();

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, String> table2 =
                (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        final KTableImpl<String, String, Integer> table1Mapped =
            (KTableImpl<String, String, Integer>) table1.mapValues(Integer::new);
        final KTableImpl<String, Integer, Integer> table1MappedFiltered =
            (KTableImpl<String, Integer, Integer>) table1Mapped.filter((key, value) -> (value % 2) == 0);
        table2.join(table1MappedFiltered, (v1, v2) -> v1 + v2);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            assertEquals(2, driver.getAllStateStores().size());
        }
    }

    private void assertTopologyContainsProcessor(final Topology topology, final String processorName) {
        for (final TopologyDescription.Subtopology subtopology: topology.describe().subtopologies()) {
            for (final TopologyDescription.Node node: subtopology.nodes()) {
                if (node.name().equals(processorName)) {
                    return;
                }
            }
        }
        throw new AssertionError("No processor named '" + processorName + "'"
                + "found in the provided Topology:\n" + topology.describe());
    }

    @Test
    public void shouldCreateSourceAndSinkNodesForRepartitioningTopic() throws NoSuchFieldException, IllegalAccessException {
        final String topic1 = "topic1";
        final String storeName1 = "storeName1";

        final StreamsBuilder builder = new StreamsBuilder();

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1,
                                                                   consumed,
                                                                   Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(storeName1)
                                                                           .withKeySerde(Serdes.String())
                                                                           .withValueSerde(Serdes.String())
                );

        table1.groupBy(MockMapper.noOpKeyValueMapper())
            .aggregate(MockInitializer.STRING_INIT, MockAggregator.TOSTRING_ADDER, MockAggregator.TOSTRING_REMOVER, Materialized.as("mock-result1"));


        table1.groupBy(MockMapper.noOpKeyValueMapper())
            .reduce(MockReducer.STRING_ADDER, MockReducer.STRING_REMOVER, Materialized.as("mock-result2"));

        final Topology topology = builder.build();
        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(topology, props)) {

            assertEquals(3, driver.getAllStateStores().size());

            assertTopologyContainsProcessor(topology, "KSTREAM-SINK-0000000003");
            assertTopologyContainsProcessor(topology, "KSTREAM-SOURCE-0000000004");
            assertTopologyContainsProcessor(topology, "KSTREAM-SINK-0000000007");
            assertTopologyContainsProcessor(topology, "KSTREAM-SOURCE-0000000008");

            final Field valSerializerField = ((SinkNode) driver.getProcessor("KSTREAM-SINK-0000000003")).getClass().getDeclaredField("valSerializer");
            final Field valDeserializerField = ((SourceNode) driver.getProcessor("KSTREAM-SOURCE-0000000004")).getClass().getDeclaredField("valDeserializer");
            valSerializerField.setAccessible(true);
            valDeserializerField.setAccessible(true);

            assertNotNull(((ChangedSerializer) valSerializerField.get(driver.getProcessor("KSTREAM-SINK-0000000003"))).inner());
            assertNotNull(((ChangedDeserializer) valDeserializerField.get(driver.getProcessor("KSTREAM-SOURCE-0000000004"))).inner());
            assertNotNull(((ChangedSerializer) valSerializerField.get(driver.getProcessor("KSTREAM-SINK-0000000007"))).inner());
            assertNotNull(((ChangedDeserializer) valDeserializerField.get(driver.getProcessor("KSTREAM-SOURCE-0000000008"))).inner());
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSelectorOnToStream() {
        table.toStream(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilter() {
        table.filter(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilterNot() {
        table.filterNot(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValues() {
        table.mapValues((ValueMapper) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValueWithKey() {
        table.mapValues((ValueMapperWithKey) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSelectorOnGroupBy() {
        table.groupBy(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnJoin() {
        table.join(null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test
    public void shouldAllowNullStoreInJoin() {
        table.join(table, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerJoin() {
        table.join(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnOuterJoin() {
        table.outerJoin(null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnOuterJoin() {
        table.outerJoin(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnLeftJoin() {
        table.leftJoin(table, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherTableOnLeftJoin() {
        table.leftJoin(null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFilterWhenMaterializedIsNull() {
        table.filter((key, value) -> false, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnFilterNotWhenMaterializedIsNull() {
        table.filterNot((key, value) -> false, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnJoinWhenMaterializedIsNull() {
        table.join(table, MockValueJoiner.TOSTRING_JOINER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnLeftJoinWhenMaterializedIsNull() {
        table.leftJoin(table, MockValueJoiner.TOSTRING_JOINER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnOuterJoinWhenMaterializedIsNull() {
        table.outerJoin(table, MockValueJoiner.TOSTRING_JOINER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenTransformerSupplierIsNull() {
        table.transformValues((ValueTransformerWithKeySupplier) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenMaterializedIsNull() {
        final ValueTransformerWithKeySupplier<String, String, ?> valueTransformerSupplier = mock(ValueTransformerWithKeySupplier.class);
        table.transformValues(valueTransformerSupplier, (Materialized) null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnTransformValuesWithKeyWhenStoreNamesNull() {
        final ValueTransformerWithKeySupplier<String, String, ?> valueTransformerSupplier = mock(ValueTransformerWithKeySupplier.class);
        table.transformValues(valueTransformerSupplier, (String[]) null);
    }
}
