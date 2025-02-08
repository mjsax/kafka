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

import java.util.Optional;
import java.util.Set;

/**
 * Determine how records are distributed among the partitions in a Kafka topic.
 * If not specified, the underlying producer's partitioning strategy will be used to determine the partition.
 * Some topologies though, need more control over which records appear in each partition for some output topics.
 * For example, downstream stateful processors may want all records within a range of keys to always be delivered to
 * and handled by the same topology instance.
 * An upstream (sub-)topology producing records to that topic can use a custom <em>stream partitioner</em> to precisely
 * and consistently determine to which partition each record should be written.
 *
 * <p>A {@code StreamPartitioner} implementations should be stateless and a pure function.
 *
 * <p><strong>Caution:</strong>While it is allowed to return more than one partition number, it is not recommended and
 * should only be done in special cases, as multicasting/broadcasting records to more than one partition is not a common
 * pattern in Kafka, and could break downstream applications reading the topic.
 * In particular, the {@link org.apache.kafka.streams.StreamsBuilder Kafka Streams DSL} expect topics to be
 * properly partitioned and may compute incorrect results if records are written into multiple partitions.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 *
 * @see Topology#addSink(String, String, StreamPartitioner, String...)
 * @see org.apache.kafka.streams.kstream.Produced
 * @see org.apache.kafka.streams.kstream.Repartitioned
 * @see org.apache.kafka.streams.kstream.TableJoined
 */
public interface StreamPartitioner<K, V> {

    /**
     * Determine the partition numbers to which a record with the given key and value should be writen into,
     * for the given topic and the topic's partition count.
     *
     * <p>The returned {@link Set} should only contain numbers between zero and {@code numPartitions-1} (both inclusive).
     * Returning {@code null} or a negative partition number will result in a runtime exception crashing the
     * Kafka Streams application.
     * Returning a partition number greater than or equal to {@code numPartitions} will lead to internal errors and an
     * infinite retry loop, effectively preventing the Kafka Streams application from making progress.
     *
     * @param topic
     *        the output topic name this record is sent to
     * @param key
     *        the key of the record
     * @param value
     *         the value of the record
     * @param numPartitions
     *        the number of partitions of the output topic
     *
     * @return An {@link Optional Optional<Set>} of integers between 0 and {@code numPartitions-1}.
     *         An {@link Optional#empty() empty} optional means using the producer's partitioner.
     *         An empty {@link Set} means the record won't be sent to any partitions, i.e., the record will be dropped.
     *         If the {@link Set} contain one or more partition numbers, the record will be sent to all the
     *         computed partitions.
     * */
    Optional<Set<Integer>> partitions(String topic, K key, V value, int numPartitions);
}
