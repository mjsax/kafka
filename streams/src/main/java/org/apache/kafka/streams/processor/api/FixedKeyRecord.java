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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.Objects;

/**
 * A data class representing an incoming record with fixed key for processing in a {@link FixedKeyProcessor}
 * or a record to {@link FixedKeyProcessorContext#forward(FixedKeyRecord) forward} to downstream processors.
 *
 * <p>This class encapsulates all the data attributes of a record: the key, value, timestamp and any headers.
 * The key is not allowed to be changed.
 *
 * <p>This class is immutable, though the objects referenced in the attributes of this class may themselves be mutable.
 * Mutating the key would be a contract violation and is not allowed.
 *
 * @param <K> the type of the fixed key
 * @param <V> the type of the value
 */
public final class FixedKeyRecord<K, V> {

    private final K key;
    private final V value;
    private final long timestamp;
    private final Headers headers;

    FixedKeyRecord(final K key, final V value, final long timestamp, final Headers headers) {
        this.key = key;
        this.value = value;
        if (timestamp < 0) {
            throw new StreamsException(
                "Malformed Record",
                new IllegalArgumentException("Timestamp may not be negative. Got: " + timestamp)
            );
        }
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Return the key of the record. May be {@code null}.
     *
     * @return The key of the record.
     */
    public K key() {
        return key;
    }

    /**
     * Return the value of the record. May be {@code null}.
     *
     * @return The value of the record.
     */
    public V value() {
        return value;
    }

    /**
     * Return the timestamp of the record. Will never be negative.
     *
     * @return The timestamp of the record.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The headers of the record. Will never be {@code null}.
     *
     * @return The headers of the record.
     */
    public Headers headers() {
        return headers;
    }

    /**
     * Return a "deep copy" of the record, with a new value.
     *
     * <p>This method makes a "deep copy" of the {@code FixedKeyRecord} only, but does not deep copy the key, value,
     * or headers objects.
     * See {@link FixedKeyProcessorContext#forward(FixedKeyRecord)} for considerations around mutability of keys,
     * values, and headers.
     *
     * @param value
     *        the value of the result record
     *
     * @param <NewV> the type of the new record's value
     *
     * @return A new {@code FixedKeyRecord} instance with all the same attributes (except that the value is replaced).
     */
    public <NewV> FixedKeyRecord<K, NewV> withValue(final NewV value) {
        return new FixedKeyRecord<>(key, value, timestamp, headers);
    }

    /**
     * Return a "deep copy" of the record, with a new timestamp.
     *
     * <p>This method makes a "deep copy" of the {@code FixedKeyRecord} only, but does not deep copy the key, value,
     * or headers objects.
     * See {@link FixedKeyProcessorContext#forward(FixedKeyRecord)} for considerations around mutability of keys,
     * values, and headers.
     *
     * @param timestamp
     *        the timestamp of the result record
     *
     * @return A new {@code FixedKeyRecord} instance with all the same attributes (except that the timestamp is replaced).
     */
    public FixedKeyRecord<K, V> withTimestamp(final long timestamp) {
        return new FixedKeyRecord<>(key, value, timestamp, headers);
    }

    /**
     * Return a "deep copy" of the record, with new headers.
     *
     * <p>This method makes a "deep copy" of the {@code FixedKeyRecord} only, but does not deep copy the key, value,
     * or headers objects.
     * See {@link FixedKeyProcessorContext#forward(FixedKeyRecord)} for considerations around mutability of keys,
     * values, and headers.
     *
     * @param headers
     *        the headers of the result record; may be null, which will cause subsequent calls to {@link #headers()}
     *        to return a not-{@code null}, empty, {@link Headers} collection.
     *
     * @return A new {@code FixedKeyRecord} instance with all the same attributes (except that the headers are replaced).
     */
    public FixedKeyRecord<K, V> withHeaders(final Headers headers) {
        return new FixedKeyRecord<>(key, value, timestamp, headers);
    }

    @Override
    public String toString() {
        return "FixedKeyRecord{" +
            "key=" + key +
            ", value=" + value +
            ", timestamp=" + timestamp +
            ", headers=" + headers +
            '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FixedKeyRecord<?, ?> record = (FixedKeyRecord<?, ?>) o;
        return timestamp == record.timestamp &&
            Objects.equals(key, record.key) &&
            Objects.equals(value, record.value) &&
            Objects.equals(headers, record.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, timestamp, headers);
    }
}