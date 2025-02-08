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

/**
 * An interface that allows to dynamically compute the name of a Kafka topic a
 * {@link org.apache.kafka.streams.Topology#addSink(String, TopicNameExtractor, String...) sink node} writes into.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface TopicNameExtractor<K, V> {

    /**
     * Extracts the topic name to write a record into.
     * The topic must already exist, since Kafka Streams will not create the topic.
     * Returning {@code null} as topic name is invalid and will result in a runtime exception.
     *
     * @param key
     *        the record key
     * @param value
     *        the record value
     * @param recordContext
     *        current context metadata of the record
     *
     * @return The topic name (cannot be {@code null}) this record should be writen into.
     */
    String extract(final K key, final V value, final RecordContext recordContext);
}
