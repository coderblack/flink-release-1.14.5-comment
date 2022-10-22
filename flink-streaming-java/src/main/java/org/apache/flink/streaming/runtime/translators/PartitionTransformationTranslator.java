/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransformationTranslator} for the {@link PartitionTransformation}.
 *
 * @param <OUT> The type of the elements that result from the {@code PartitionTransformation} being
 *     translated.
 */
@Internal
public class PartitionTransformationTranslator<OUT>
        extends SimpleTransformationTranslator<OUT, PartitionTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final PartitionTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final PartitionTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    private Collection<Integer> translateInternal(
            final PartitionTransformation<OUT> transformation, final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        final StreamGraph streamGraph = context.getStreamGraph();

        final List<Transformation<?>> parentTransformations = transformation.getInputs();

        //多易教育: 检查上游transformation只能有一个
        checkState(
                parentTransformations.size() == 1,
                "Expected exactly one input transformation but found "
                        + parentTransformations.size());
        //多易教育: 取到上游transformation
        final Transformation<?> input = parentTransformations.get(0);

        List<Integer> resultIds = new ArrayList<>();

        //多易教育:
        // 这里调用的是 streamGraphGenerator.alreadyTransformed.get(transformation)
        // => 已转译完成的transformationId
        for (Integer inputId : context.getStreamNodeIds(input)) {
            //多易教育: 生成一个新的nodeId作为本虚拟节点的虚拟id
            final int virtualId = Transformation.getNewNodeId();
            //多易教育: 添加虚拟节点
            // Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>> virtualPartitionNodes
            // StreamGraph.virtualPartitionNodes.put(virtualId, new Tuple3<>(originalId, partitioner, exchangeMode));
            streamGraph.addVirtualPartitionNode(
                    inputId,  // 上游transformationId ,即所谓 originalId
                    virtualId,  // 本虚拟节点id
                    transformation.getPartitioner(),  // 分区器partitioner
                    transformation.getExchangeMode());
            resultIds.add(virtualId);
        }
        return resultIds;
    }
}
