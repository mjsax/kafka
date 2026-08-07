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
package org.apache.kafka.streams.integration.utils;

import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A broker-side task assignor that hands one member a task of a subtopology that does not exist in the topology.
 *
 * <p>A member assignment is a {@code Map<String, Set<Integer>>}, so the subtopology id is an arbitrary string that
 * nothing in the assignor API constrains to name a real subtopology, and the broker does not validate assignor output.
 * A custom assignor with a defect can therefore produce this, which is what makes it worth reproducing.
 *
 * <p>While the group has a single member, the assignment is the ordinary one: every task of the first subtopology goes
 * to that member. Once a second member joins, the members are ordered by id and the last one is additionally given
 * task {@code doesNotExist_0}, on top of its share of the real tasks.
 */
public class UnknownSubtopologyAssignor implements TaskAssignor {

    public static final String UNKNOWN_SUBTOPOLOGY_ID = "doesNotExist";

    @Override
    public String name() {
        return "unknown-subtopology";
    }

    @Override
    public GroupAssignment assign(final GroupSpec groupSpec,
                                  final TopologyDescriber topologyDescriber) throws TaskAssignorException {
        final List<String> memberIds = List.copyOf(new TreeSet<>(groupSpec.memberIds()));
        if (memberIds.isEmpty()) {
            return new GroupAssignment(Map.of());
        }

        final String subtopologyId = new TreeSet<>(topologyDescriber.subtopologies()).first();
        final int numberOfTasks = topologyDescriber.maxNumInputPartitions(subtopologyId);

        final Map<String, Map<String, Set<Integer>>> activeTasksByMember = new HashMap<>();
        for (final String memberId : memberIds) {
            activeTasksByMember.put(memberId, new HashMap<>());
        }
        for (int task = 0; task < numberOfTasks; task++) {
            final String owner = memberIds.get(task % memberIds.size());
            activeTasksByMember.get(owner).computeIfAbsent(subtopologyId, __ -> new TreeSet<>()).add(task);
        }

        // Poison a member that currently runs nothing, so that the bad assignment is the first one it ever receives and
        // it therefore never starts any task at all. Poisoning a member that already runs tasks would be far less
        // visible: it simply keeps serving the assignment it reconciled earlier.
        if (memberIds.size() > 1) {
            memberIds.stream()
                .filter(memberId -> groupSpec.memberAssignmentState(memberId).activeTasks().isEmpty())
                .reduce((first, second) -> second)
                .ifPresent(victim -> activeTasksByMember.get(victim).put(UNKNOWN_SUBTOPOLOGY_ID, Set.of(0)));
        }

        final Map<String, MemberAssignment> assignment = new HashMap<>();
        activeTasksByMember.forEach((memberId, activeTasks) ->
            assignment.put(memberId, new MemberAssignment(activeTasks, new HashMap<>())));
        return new GroupAssignment(assignment);
    }
}
