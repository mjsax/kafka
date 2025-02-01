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
package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
 */
public class StreamStreamJoinNode<K, VLeft, VRight, VOut> extends BaseJoinProcessorNode<K, VLeft, VRight, VOut> {
    private final String thisWindowedStreamProcessorName;
    private final String otherWindowedStreamProcessorName;
    private final ProcessorParameters<K, VLeft, K, VOut> selfJoinProcessorParameters;
    private boolean isSelfJoin;

    private StreamStreamJoinNode(final String nodeName,
                                 final ValueJoinerWithKey<? super K, ? super VLeft, ? super VRight, ? extends VOut> valueJoiner,
                                 final ProcessorParameters<K, VLeft, K, VOut> joinThisProcessorParameters,
                                 final ProcessorParameters<K, VRight, K, VOut> joinOtherProcessParameters,
                                 final ProcessorParameters<K, VOut, K, VOut> joinMergeProcessorParameters,
                                 final ProcessorParameters<K, VLeft, K, VOut> selfJoinProcessorParameters,
                                 final String thisWindowedStreamProcessorName,
                                 final String otherWindowedStreamProcessorName) {

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessParameters,
              joinMergeProcessorParameters,
              null,
              null);

        this.thisWindowedStreamProcessorName = thisWindowedStreamProcessorName;
        this.otherWindowedStreamProcessorName =  otherWindowedStreamProcessorName;
        this.selfJoinProcessorParameters = selfJoinProcessorParameters;
    }

    @Override
    public String toString() {
        return "StreamStreamJoinNode{" +
            "thisWindowedStreamProcessorName=" + thisWindowedStreamProcessorName +
            ", otherWindowedStreamProcessorName=" + otherWindowedStreamProcessorName +
               "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {

        final String thisProcessorName = thisProcessorParameters().processorName();
        final String otherProcessorName = otherProcessorParameters().processorName();

        if (isSelfJoin) {
            selfJoinProcessorParameters.addProcessorTo(topologyBuilder, thisWindowedStreamProcessorName);
        } else {
            thisProcessorParameters().addProcessorTo(topologyBuilder, thisWindowedStreamProcessorName);
            otherProcessorParameters().addProcessorTo(topologyBuilder, otherWindowedStreamProcessorName);

            mergeProcessorParameters().addProcessorTo(topologyBuilder, thisProcessorName, otherProcessorName);
        }
    }

    public void setSelfJoin() {
        this.isSelfJoin = true;
    }

    public boolean getSelfJoin() {
        return isSelfJoin;
    }

    public String thisWindowedStreamProcessorName() {
        return thisWindowedStreamProcessorName;
    }

    public String otherWindowedStreamProcessorName() {
        return otherWindowedStreamProcessorName;
    }

    public static <K, V1, V2, VR> StreamStreamJoinNodeBuilder<K, V1, V2, VR> streamStreamJoinNodeBuilder() {
        return new StreamStreamJoinNodeBuilder<>();
    }

    public static final class StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> {

        private String nodeName;
        private ValueJoinerWithKey<? super K, ? super LeftValue, ? super RightValue, ? extends VResult> valueJoiner;
        private ProcessorParameters<K, LeftValue, K, VResult> joinThisProcessorParameters;
        private ProcessorParameters<K, RightValue, K, VResult> joinOtherProcessorParameters;
        private ProcessorParameters<K, VResult, K, VResult> joinMergeProcessorParameters;
        private ProcessorParameters<K, LeftValue, K, VResult> selfJoinProcessorParameters;
        private String thisWindowedStreamProcessorName;
        private String otherWindowedStreamProcessorName;

        private StreamStreamJoinNodeBuilder() {
        }

        public StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> withValueJoiner(
            final ValueJoinerWithKey<? super K, ? super LeftValue, ? super RightValue, ? extends VResult> valueJoiner
        ) {
            this.valueJoiner = valueJoiner;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> withJoinThisProcessorParameters(
            final ProcessorParameters<K, LeftValue, K, VResult> joinThisProcessorParameters
        ) {
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> withJoinOtherProcessorParameters(
            final ProcessorParameters<K, RightValue, K, VResult> joinOtherProcessParameters
        ) {
            this.joinOtherProcessorParameters = joinOtherProcessParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> withSelfJoinProcessorParameters(
            final ProcessorParameters<K, LeftValue, K, VResult> selfJoinProcessorParameters
        ) {
            this.selfJoinProcessorParameters = selfJoinProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> withJoinMergeProcessorParameters(
            final ProcessorParameters<K, VResult, K, VResult> joinMergeProcessorParameters
        ) {
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> withThisWindowedStreamProcessorName(
            final String thisWindowedStreamProcessorName
        ) {
            this.thisWindowedStreamProcessorName = thisWindowedStreamProcessorName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, LeftValue, RightValue, VResult> withOtherWindowedStreamProcessorName(
            final String otherWindowedStreamProcessorName
        ) {
            this.otherWindowedStreamProcessorName = otherWindowedStreamProcessorName;
            return this;
        }

        public StreamStreamJoinNode<K, LeftValue, RightValue, VResult> build() {
            return new StreamStreamJoinNode<>(
                nodeName,
                valueJoiner,
                joinThisProcessorParameters,
                joinOtherProcessorParameters,
                joinMergeProcessorParameters,
                selfJoinProcessorParameters,
                thisWindowedStreamProcessorName,
                otherWindowedStreamProcessorName
            );
        }
    }
}
