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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.TopologyException;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.junit.Assert.fail;

public class NamedTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionGivenNullName() {
        Named.as(null);
    }

    @Test
    public void shouldThrowExceptionOnInvalidTopicNames() {
        final char[] longString = new char[250];
        Arrays.fill(longString, 'a');
        final String[] invalidNames = {"", "foo bar", "..", "foo:bar", "foo=bar", ".", new String(longString)};

        for (final String name : invalidNames) {
            try {
                Named.validate(name);
                fail("No exception was thrown for named with invalid name: " + name);
            } catch (final TopologyException e) {
                // success
            }
        }
    }

    @Test
    public void test() {
        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupByKey().count();

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupByKey(Grouped.as("grpBy")).count();

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupByKey().count(Named.as("cnt"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupByKey().count(Materialized.as("store"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupByKey(Grouped.as("grp")).count(Named.as("cnt"), Materialized.as("store"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();
    }

    @Test
    public void test2() {
        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupBy((k, v) -> k).count();

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupBy((k, v) -> k, Grouped.as("grpBy")).count();

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupBy((k, v) -> k).count(Named.as("cnt"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupBy((k, v) -> k).count(Materialized.as("store"));

            System.out.println(builder.build().describe().toString());
        }

        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream("topic").groupBy((k, v) -> k, Grouped.as("grp")).count(Named.as("cnt"), Materialized.as("store"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();
    }

    @Test
    public void test3() {
        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.stream("topic1");
            final KStream<String, String> s2 = builder.stream("topic2");

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.stream("topic1");
            final KStream<String, String> s2 = builder.stream("topic2");

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.as("store"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.stream("topic1");
            final KStream<String, String> s2 = builder.stream("topic2");

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.<String, String, String>as(null).withName("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.stream("topic1");
            final KStream<String, String> s2 = builder.stream("topic2");

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.<String, String, String>as("store").withName("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();
    }

    @Test
    public void test4() {
        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KStream<String, String> s2 = builder.stream("topic2");

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KStream<String, String> s2 = builder.stream("topic2");

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.as("store"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KStream<String, String> s2 = builder.stream("topic2");

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.<String, String, String>as(null).withName("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KStream<String, String> s2 = builder.stream("topic2");

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.<String, String, String>as("store").withName("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();
    }

    @Test
    public void test5() {
        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.stream("topic1");
            final KStream<String, String> s2 = builder.<String, String>stream("topic2").selectKey((k, v) -> k);

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.stream("topic1");
            final KStream<String, String> s2 = builder.<String, String>stream("topic2").selectKey((k, v) -> k);

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.as("store"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.stream("topic1");
            final KStream<String, String> s2 = builder.<String, String>stream("topic2").selectKey((k, v) -> k);

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.<String, String, String>as(null).withName("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.stream("topic1");
            final KStream<String, String> s2 = builder.<String, :wqString>stream("topic2").selectKey((k, v) -> k);

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.<String, String, String>as("store").withName("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();
    }

    @Test
    public void test6() {
        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KStream<String, String> s2 = builder.<String, String>stream("topic2").selectKey((k, v) -> k);

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KStream<String, String> s2 = builder.<String, String>stream("topic2").selectKey((k, v) -> k);

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.as("store"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KStream<String, String> s2 = builder.<String, String>stream("topic2").selectKey((k, v) -> k);

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.<String, String, String>as(null).withName("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s1 = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KStream<String, String> s2 = builder.<String, String>stream("topic2").selectKey((k, v) -> k);

            s1.join(s2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ZERO), StreamJoined.<String, String, String>as("store").withName("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();
    }

    @Test
    public void test7() {
        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.stream("topic1");
            final KTable<String, String> t = builder.table("topic2");

            s.join(t, (v1, v2) -> v1 + v2);

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.stream("topic1");
            final KTable<String, String> t = builder.table("topic2", Consumed.as("table"));

            s.join(t, (v1, v2) -> v1 + v2);

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.stream("topic1");
            final KTable<String, String> t = builder.table("topic2", Materialized.as("store"));

            s.join(t, (v1, v2) -> v1 + v2);

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.stream("topic1");
            final KTable<String, String> t = builder.table("topic2");

            s.join(t, (v1, v2) -> v1 + v2, Joined.as("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.stream("topic1");
            final KTable<String, String> t = builder.table("topic2", Consumed.as("table"), Materialized.as("store"));

            s.join(t, (v1, v2) -> v1 + v2);

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.stream("topic1");
            final KTable<String, String> t = builder.table("topic2", Consumed.as("table"));

            s.join(t, (v1, v2) -> v1 + v2, Joined.as("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.stream("topic1");
            final KTable<String, String> t = builder.table("topic2", Materialized.as("store"));

            s.join(t, (v1, v2) -> v1 + v2, Joined.as("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.stream("topic1");
            final KTable<String, String> t = builder.table("topic2", Consumed.as("table"), Materialized.as("store"));

            s.join(t, (v1, v2) -> v1 + v2, Joined.as("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();
    }

    @Test
    public void test8() {
        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.<String, String>stream("topic1").selectKey((k, v) -> k, Named.as("key"));
            final KTable<String, String> t = builder.table("topic2");

            s.join(t, (v1, v2) -> v1 + v2);

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KTable<String, String> t = builder.table("topic2", Consumed.as("table"));

            s.join(t, (v1, v2) -> v1 + v2);

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KTable<String, String> t = builder.table("topic2", Materialized.as("store"));

            s.join(t, (v1, v2) -> v1 + v2);

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KTable<String, String> t = builder.table("topic2");

            s.join(t, (v1, v2) -> v1 + v2, Joined.as("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KTable<String, String> t = builder.table("topic2", Consumed.as("table"), Materialized.as("store"));

            s.join(t, (v1, v2) -> v1 + v2);

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KTable<String, String> t = builder.table("topic2", Consumed.as("table"));

            s.join(t, (v1, v2) -> v1 + v2, Joined.as("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KTable<String, String> t = builder.table("topic2", Materialized.as("store"));

            s.join(t, (v1, v2) -> v1 + v2, Joined.as("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();

        {
            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> s = builder.<String, String>stream("topic1").selectKey((k, v) -> k);
            final KTable<String, String> t = builder.table("topic2", Consumed.as("table"), Materialized.as("store"));

            s.join(t, (v1, v2) -> v1 + v2, Joined.as("join"));

            System.out.println(builder.build().describe().toString());
        }
        System.out.println();
        System.out.println();
    }


}