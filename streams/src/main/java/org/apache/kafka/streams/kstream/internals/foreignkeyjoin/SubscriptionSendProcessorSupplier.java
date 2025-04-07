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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.internals.Murmur3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Supplier;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_AND_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.DELETE_KEY_NO_PROPAGATE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE;
import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapper.Instruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE;

public class SubscriptionSendProcessorSupplier<KLeft, VLeft, KRight>
    implements ProcessorSupplier<KLeft, Change<VLeft>, KRight, SubscriptionWrapper<KLeft>> {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSendProcessorSupplier.class);

    private final ForeignKeyExtractor<? super KLeft, ? super VLeft, ? extends KRight> foreignKeyExtractor;
    private final Supplier<String> foreignKeySerdeTopicSupplier;
    private final Supplier<String> valueSerdeTopicSupplier;
    private final boolean leftJoin;
    private Serializer<KRight> foreignKeySerializer;
    private Serializer<VLeft> valueSerializer;
    private boolean useVersionedSemantics;

    public SubscriptionSendProcessorSupplier(final ForeignKeyExtractor<? super KLeft, ? super VLeft, ? extends KRight> foreignKeyExtractor,
                                             final Supplier<String> foreignKeySerdeTopicSupplier,
                                             final Supplier<String> valueSerdeTopicSupplier,
                                             final Serde<KRight> foreignKeySerde,
                                             final Serializer<VLeft> valueSerializer,
                                             final boolean leftJoin) {
        this.foreignKeyExtractor = foreignKeyExtractor;
        this.foreignKeySerdeTopicSupplier = foreignKeySerdeTopicSupplier;
        this.valueSerdeTopicSupplier = valueSerdeTopicSupplier;
        this.valueSerializer = valueSerializer;
        this.leftJoin = leftJoin;
        foreignKeySerializer = foreignKeySerde == null ? null : foreignKeySerde.serializer();
    }

    @Override
    public Processor<KLeft, Change<VLeft>, KRight, SubscriptionWrapper<KLeft>> get() {
        return new UnbindChangeProcessor();
    }

    public void setUseVersionedSemantics(final boolean useVersionedSemantics) {
        this.useVersionedSemantics = useVersionedSemantics;
    }

    // VisibleForTesting
    public boolean isUseVersionedSemantics() {
        return useVersionedSemantics;
    }

    private class UnbindChangeProcessor extends ContextualProcessor<KLeft, Change<VLeft>, KRight, SubscriptionWrapper<KLeft>> {

        private Sensor droppedRecordsSensor;
        private String foreignKeySerdeTopic;
        private String valueSerdeTopic;
        private long[] recordHash;

        @SuppressWarnings({"unchecked", "resource"})
        @Override
        public void init(final ProcessorContext<KRight, SubscriptionWrapper<KLeft>> context) {
            super.init(context);
            foreignKeySerdeTopic = foreignKeySerdeTopicSupplier.get();
            valueSerdeTopic = valueSerdeTopicSupplier.get();
            // get default key serde if it wasn't supplied directly at construction
            if (foreignKeySerializer == null) {
                foreignKeySerializer = (Serializer<KRight>) context.keySerde().serializer();
            }
            if (valueSerializer == null) {
                valueSerializer = (Serializer<VLeft>) context.valueSerde().serializer();
            }
            droppedRecordsSensor = TaskMetrics.droppedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics()
            );
        }

        @Override
        public void process(final Record<KLeft, Change<VLeft>> record) {
            // clear cashed hash from previous record
            recordHash = null;
            // drop out-of-order records from versioned tables (cf. KIP-914)
            if (useVersionedSemantics && !record.value().isLatest) {
                LOG.info("Skipping out-of-order record from versioned table while performing table-table join.");
                droppedRecordsSensor.record();
                return;
            }
            if (leftJoin) {
                leftJoinInstructions(record);
            } else {
                defaultJoinInstructions(record);
            }
        }

        private void leftJoinInstructions(final Record<KLeft, Change<VLeft>> record) {
//            // my fix simplified
//            final VLeft oldValue = record.value().oldValue;
//            final VLeft newValue = record.value().newValue;
//
//            if (oldValue == null && newValue == null) {
//                // no output for idempotent left hand side deletes
//                return;
//            }
//
//            final KRight oldForeignKey = oldValue == null ? null : foreignKeyExtractor.extract(record.key(), oldValue);
//            final KRight newForeignKey = newValue == null ? null : foreignKeyExtractor.extract(record.key(), newValue);
//
//            // this sound incorrect -- even if the old and new FK are the same, the _left row_ might have changed
//            // -> thus we still need to send the new subscription to update the result
//            // update subscription only, if the new left value is different from the old left value,
//            // to avoid unnecessary idempotent updates
//            // there is still a difference for this case -- we send the subscription to the same partition,
//            // and thus, we only need to send a single subscription update, not two
//            // (even if this is just a minor optimization: the first "unsubscribe" would not result in a response anyway,
//            // so we only take of some load from the subscription topic, and the right hand side store/processor).
//            // we need to send two if the FK changes though
////            if (Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
////                return;
////            }
//
//            // Let's not do this -- we have emit-on-update semantics, not emit-on-change
//            // KIP-557 proposed this, but was rolled back
//            // it's also questionable if emit-on-change would even be correct for versioned tables
////            if (Arrays.equals(serializeLeftValue(newValue), serializeLeftValue(oldValue))) {
////                return;
////            }
//
//            final boolean unsubscribe = oldForeignKey != null;
//            if (unsubscribe) {
//                // this may lead to unnecessary tombstones, if we delete an existing key,
//                // which did not join previously;
//                // however, we cannot avoid it as we have no means to know if the old FK joined or not
//                forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
//            }
//
//            // for all cases, insert, update, and delete, we send a new subscription
//            // we need to get a response back for all cases to always produce a left-join result
//            //
//            // note: for delete, `newForeignKey` is null, what is a "hack"
//            // no actual subscription will be added for null-FK, but we still get the response back we need
//            //
//            // this may lead to unnecessary tombstones, if we delete an existing key,
//            // which did not join previously;
//            // however, we cannot avoid it as we have no means to know if the old FK joined or not
//            forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);



            // my fix
//            final KRight oldForeignKey = record.value().oldValue == null ? null : foreignKeyExtractor.extract(record.key(), record.value().oldValue);
//            final boolean unsubscribe = oldForeignKey != null;
//
//            // if left row is inserted or updated, subscribe to new FK (if new FK is valid)
//            if (record.value().newValue != null) {
//                final KRight newForeignKey = foreignKeyExtractor.extract(record.key(), record.value().newValue);
//
//                if (newForeignKey == null) {
//                    //logSkippedRecordDueToNullForeignKey();
//                    if (unsubscribe) {
//                        // no-propagate delete for FK-join
//                        forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
//                    }
//                    // new for FK
//                    forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//                } else {
//                    // regular insert/update
//
//                    // update subscription only, if the new FK is different from the old FK,
//                    // to avoid unnecessary idempotent updates
//                    if (Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
//                        return;
//                    }
//
//                    if (unsubscribe) {
//                        // update case
//
//                        // delete old subscription
//                        // we don't need any response, as we only want a response from the new subscription
//                        forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
//
//                        // subscribe to new key (note, could be on a different task/node than old key)
//                        // additionally, propagate null if no FK is found there,
//                        // since we must delete previous result (if any)
//                        //
//                        // this may lead to unnecessary tombstones if the old FK did not join
//                        // and the new FK key does not join either;
//                        // however, we cannot avoid it because old and new FK may be on different tasks/nodes,
//                        // and thus, we cannot verify if the tombstone is needed or not
//                        forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//                    } else {
//                        // insert
//
//                        // subscribe to new key
//                        // don't propagate null if no FK is found there,
//                        // for inserts, we know that there is need to delete any previous result
//
//                        // always use null-propagate for FK-join
//                        forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//                    }
//                }
//            } else {
//                // left row is deleted
//                if (unsubscribe) {
//                    // this may lead to unnecessary tombstones, if we delete an existing key,
//                    // which did not join previously;
//                    // however, we cannot avoid it as we have no means to know if the old FK joined or not
//
//                    // always use no-propagate for FK-join
//                    forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
//                }
//
//                // always delete and propagate for FK
//                forward(record, null, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//           }

            // -------------------------------------------

//            // K16394 fix
//            if (record.value().oldValue != null) {
//                final KRight oldForeignKey = foreignKeyExtractor.extract(record.key(), record.value().oldValue);
//                final KRight newForeignKey = record.value().newValue == null ? null : foreignKeyExtractor.extract(record.key(), record.value().newValue);
//                if (oldForeignKey == null) {
//                    forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//                } else if (newForeignKey == null) {
//                    forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
//                } else if (!Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
//                    forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
//                    forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//                } else {
//                    forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//                }
//            } else if (record.value().newValue != null) {
//                final KRight newForeignKey = foreignKeyExtractor.extract(record.key(), record.value().newValue);
//                forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//            }

            // -------------------------------------------

            // original
            if (record.value().oldValue != null) {
                final KRight oldForeignKey = foreignKeyExtractor.extract(record.key(), record.value().oldValue);
                final KRight newForeignKey = record.value().newValue == null ? null : foreignKeyExtractor.extract(record.key(), record.value().newValue);
                if (oldForeignKey != null && !Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
                    forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE); // K18713 fix
                }
                forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
            } else if (record.value().newValue != null) {
                final KRight newForeignKey = foreignKeyExtractor.extract(record.key(), record.value().newValue);
                forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
            }
        }

        private void defaultJoinInstructions(final Record<KLeft, Change<VLeft>> record) {
//            // my fix
//            final VLeft oldValue = record.value().oldValue;
//            final VLeft newValue = record.value().newValue;
//
//            final KRight oldForeignKey = oldValue == null ? null : foreignKeyExtractor.extract(record.key(), oldValue);
//            final boolean unsubscribe = oldForeignKey != null;
//
//            // if left row is inserted or updated, subscribe to new FK (if new FK is valid)
//            if (newValue != null) {
//                final KRight newForeignKey = foreignKeyExtractor.extract(record.key(), newValue);
//
//                if (newForeignKey == null) {
//                    logSkippedRecordDueToNullForeignKey();
//                    if (unsubscribe) {
//                        // delete old subscription
//                        //
//                        // this may lead to unnecessary tombstones if the old FK did not join
//                        // however, we cannot avoid it as we have no means to know if the old FK joined or not
//                        forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
//                    }
//                } else {
//                    // regular insert/update
//
//                    // update subscription only, if the new value is different from the old value,
//                    // to avoid unnecessary idempotent updates
////                    if (Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
////                        return;
////                    }
//                    if (Arrays.equals(serializeLeftValue(newValue), serializeLeftValue(oldValue))) {
//                        return;
//                    }
//
//                    if (unsubscribe) {
//                        // update case
//
//                        // delete old subscription
//                        // we don't need any response, as we only want a response from the new subscription
//                        forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
//
//                        // subscribe to new key (note, could be on a different task/node than old key)
//                        // additionally, propagate null if no FK is found so we can delete the previous result (if any)
//                        //
//                        // this may lead to unnecessary tombstones if the old FK did not join
//                        // and the new FK key does not join either;
//                        // however, we cannot avoid it as we have no means to know if the old FK joined or not
//                        forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//                    } else {
//                        // insert
//
//                        // subscribe to new key
//                        // don't propagate null if no FK is found;
//                        // for inserts, we know that there is need to delete any previous result
//                        forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
//                    }
//                }
//            } else {
//                // left row is deleted
//                if (unsubscribe) {
//                    // this may lead to unnecessary tombstones, if we delete an existing key,
//                    // which did not join previously;
//                    // however, we cannot avoid it as we have no means to know if the old FK joined or not
//                    forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
//                }
//            }

            // -------------------------------------------

            // KAFKA-16407 fix
            if (record.value().oldValue != null) {
                final KRight oldForeignKey = foreignKeyExtractor.extract(record.key(), record.value().oldValue);
                final KRight newForeignKey = record.value().newValue == null ? null : foreignKeyExtractor.extract(record.key(), record.value().newValue);

                if (oldForeignKey == null && newForeignKey == null) {
                    logSkippedRecordDueToNullForeignKey();
                } else if (oldForeignKey == null) {
                    forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
                } else if (newForeignKey == null) {
                    forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
                } else if (!Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
                    //Different Foreign Key - delete the old key value and propagate the new one.
                    //Delete it from the oldKey's state store
                    forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
                    //Add to the newKey's state store. Additionally, propagate null if no FK is found there,
                    //since we must "unset" any output set by the previous FK-join. This is true for both INNER
                    //and LEFT join.
                    forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
                } else { // unchanged FK
                    forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
                }
            } else if (record.value().newValue != null) {
                final KRight newForeignKey = foreignKeyExtractor.extract(record.key(), record.value().newValue);
                if (newForeignKey == null) {
                    logSkippedRecordDueToNullForeignKey();
                } else {
                    forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
                }
            }

            // -------------------------------------------

//            // original
//            if (record.value().oldValue != null) {
//                final KRight oldForeignKey = record.value().oldValue == null ? null : foreignKeyExtractor.extract(record.key(), record.value().oldValue);
//                if (oldForeignKey == null) {
//                    logSkippedRecordDueToNullForeignKey();
//                    return;
//                }
//                if (record.value().newValue != null) {
//                    final KRight newForeignKey = record.value().newValue == null ? null : foreignKeyExtractor.extract(record.key(), record.value().newValue);
//                    if (newForeignKey == null) {
//                        logSkippedRecordDueToNullForeignKey();
//                        return;
//                    }
//                    if (!Arrays.equals(serialize(newForeignKey), serialize(oldForeignKey))) {
//                        //Different Foreign Key - delete the old key value and propagate the new one.
//                        //Delete it from the oldKey's state store
//                        forward(record, oldForeignKey, DELETE_KEY_NO_PROPAGATE);
//                    }
//                    //Add to the newKey's state store. Additionally, propagate null if no FK is found there,
//                    //since we must "unset" any output set by the previous FK-join. This is true for both INNER
//                    //and LEFT join.
//                    forward(record, newForeignKey, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
//                } else {
//                    forward(record, oldForeignKey, DELETE_KEY_AND_PROPAGATE);
//                }
//            } else if (record.value().newValue != null) {
//                final KRight newForeignKey = foreignKeyExtractor.extract(record.key(), record.value().newValue);
//                if (newForeignKey == null) {
//                    logSkippedRecordDueToNullForeignKey();
//                } else {
//                    forward(record, newForeignKey, PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
//                }
//            }
        }

        private byte[] serialize(final KRight key) {
            return foreignKeySerializer.serialize(foreignKeySerdeTopic, key);
        }

        private byte[] serializeLeftValue(final VLeft value) {
            return valueSerializer.serialize(valueSerdeTopic, value);
        }

        private void forward(final Record<KLeft, Change<VLeft>> record, final KRight foreignKey, final Instruction deleteKeyNoPropagate) {
            final SubscriptionWrapper<KLeft> wrapper = new SubscriptionWrapper<>(
                hash(record),
                deleteKeyNoPropagate,
                record.key(),
                context().recordMetadata().get().partition()
            );
            context().forward(record.withKey(foreignKey).withValue(wrapper));
        }

        private long[] hash(final Record<KLeft, Change<VLeft>> record) {
            if (recordHash == null) {
                recordHash = record.value().newValue == null
                    ? null
                    : Murmur3.hash128(valueSerializer.serialize(valueSerdeTopic, record.value().newValue));
            }
            return recordHash;
        }

        private void logSkippedRecordDueToNullForeignKey() {
            if (context().recordMetadata().isPresent()) {
                final RecordMetadata recordMetadata = context().recordMetadata().get();
                LOG.warn(
                    "Skipping record due to null foreign key. topic=[{}] partition=[{}] offset=[{}]",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                );
            } else {
                LOG.warn("Skipping record due to null foreign key. Topic, partition, and offset not known.");
            }
            droppedRecordsSensor.record();
        }
    }
}
