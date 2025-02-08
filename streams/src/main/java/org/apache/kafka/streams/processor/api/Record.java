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
 * A data class representing an incoming record for processing in a {@link Processor} or a record to
 * {@link ProcessorContext#forward(Record) forward} to downstream processors.
 *
 * <p>This class encapsulates all the data attributes of a record: the key, value, timestamp and any headers.
 *
 * <p>This class is immutable, though the objects referenced in the attributes of this class may themselves be mutable.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class Record<K, V> {
    private final K key;
    private final V value;
    private final long timestamp;
    private final Headers headers;

    /**
     * Create a new record with the provided key, value, timestamp, and headers.
     *
     * <p>Note: this constructor makes a copy of the {@code headers} argument.
     * See {@link ProcessorContext#forward(Record)} for
     * considerations around mutability of keys, values, and headers.
     *
     * @param key
     *        the key of the record; may be {@code null}
     * @param value
     *        the value of the record; may be {@code null}
     * @param timestamp
     *        the timestamp of the record; cannot be negative
     * @param headers
     *        the headers of the record; may be null, which will cause subsequent calls to {@link #headers()} to return
     *        a not-{@code null}, empty, {@link Headers} collection
     *
     * @throws IllegalArgumentException
     *         if the provide {@code timestamp} is negative.
     */
    public Record(final K key, final V value, final long timestamp, final Headers headers) {
        this.key = key;
        this.value = value;
        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be negative. Got: " + timestamp);
        }
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * See {@link #Record(Object, Object, long, Headers)}.
     *
     * <p>Create a new record with empty headers.
     */
    public Record(final K key, final V value, final long timestamp) {
        this(key, value, timestamp, null);
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
     * Return a "deep copy" of the record, with a new key.
     *
     * <p>This method makes a "deep copy" of the {@code Record} only, but does not deep copy the key, value, or headers
     * objects.
     * See {@link ProcessorContext#forward(Record)} for considerations around mutability of keys, values, and headers.
     *
     * @param key
     *        the key of the result record; may be {@code null}
     *
     * @param <NewK> the type of the new record's key
     *
     * @return A new {@code Record} instance with all the same attributes (except that the key is replaced).
     */
    public <NewK> Record<NewK, V> withKey(final NewK key) {
        return new Record<>(key, value, timestamp, headers);
    }

    /**
     * Return a "deep copy" of the record, with a new value.
     *
     * <p>This method makes a "deep copy" of the {@code Record} only, but does not deep copy the key, value, or headers
     * objects.
     * See {@link ProcessorContext#forward(Record)} for considerations around mutability of keys, values, and headers.
     *
     * @param value
     *        the value of the result record; may be {@code null}
     *
     * @param <NewV> the type of the new record's value
     *
     * @return A new {@code Record} instance with all the same attributes (except that the value is replaced).
     */
    public <NewV> Record<K, NewV> withValue(final NewV value) {
        return new Record<>(key, value, timestamp, headers);
    }

    /**
     * Return a "deep copy" of the record, with a new timestamp.
     *
     * <p>This method makes a "deep copy" of the {@code Record} only, but does not deep copy the key, value, or headers
     * objects.
     * See {@link ProcessorContext#forward(Record)} for considerations around mutability of keys, values, and headers.
     *
     * @param timestamp
     *        the timestamp of the result record; cannot be negative
     *
     * @return A new {@code Record} instance with all the same attributes (except that the timestamp is replaced).
     *
     * @throws IllegalArgumentException
     *         if the provide {@code timestamp} is negative.
     */
    public Record<K, V> withTimestamp(final long timestamp) {
        return new Record<>(key, value, timestamp, headers);
    }

    /**
     * Return a "deep copy" of the record, with a new key.
     *
     * <p>This method makes a "deep copy" of the {@code Record} only, but does not deep copy the key, value, or headers
     * objects.
     * See {@link ProcessorContext#forward(Record)} for considerations around mutability of keys, values, and headers.
     *
     * @param headers
     *        the headers of the record; may be null, which will cause subsequent calls to {@link #headers()} to return
     *        a not-{@code null}, empty, {@link Headers} collection
     *
     * @return A new {@code Record} instance with all the same attributes (except that the headers are replaced).
     */
    public Record<K, V> withHeaders(final Headers headers) {
        return new Record<>(key, value, timestamp, headers);
    }

    @Override
    public String toString() {
        return "Record{" +
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
        final Record<?, ?> record = (Record<?, ?>) o;
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