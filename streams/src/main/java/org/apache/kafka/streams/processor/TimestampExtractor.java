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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.KTable;

/**
 * An interface that allows the Kafka Streams framework to extract a timestamp from an instance of {@link ConsumerRecord}.
 * The extracted timestamp is defined as milliseconds.
 */
public interface TimestampExtractor {

    /**
     * Extracts a timestamp from a record.
     * The timestamp must be positive to be considered valid.
     * Returning a negative timestamp is not an error, but will cause the record to be dropped.
     * In case the record contains a negative timestamp and this is considered a fatal error for the application,
     * throwing a {@link RuntimeException} instead of returning the timestamp is a valid option.
     * For this case, Kafka Streams will stop processing and shut down to allow you to investigate the root cause of the
     * negative timestamp.
     *
     * <p>The timestamp extractor implementation must be stateless.
     * The extracted timestamp must represent UNIX epoch time, i.e., milliseconds since midnight, January 1, 1970 UTC.
     *
     * <p>It is important to note that this timestamp may become the message timestamp for any messages sent to
     * changelogs updated by {@link KTable}s and joins.
     * The message timestamp is used for log retention and log rolling, so using nonsensical values may result in
     * excessive log rolling and therefore broker performance degradation.
     *
     * @param record
     *        input record to be processed by Kafka Streams
     * @param partitionTime
     *        the highest extracted valid timestamp of the current record's partition (could be {@code -1} if unknown)
     *
     * @return The timestamp for the record.
     */
    long extract(ConsumerRecord<Object, Object> record, long partitionTime);
}
