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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.checkpoint.StateObjectCollection.emptyIfNull;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singletonOrEmpty;

/**
 * This class finalizes {@link OperatorSnapshotFutures}. Each object is created with a {@link
 * OperatorSnapshotFutures} that is executed. The object can then deliver the results from the
 * execution as {@link OperatorSubtaskState}.
 */
public class OperatorSnapshotFinalizer {

    /** Primary replica of the operator subtask state for report to JM. */
    private final OperatorSubtaskState jobManagerOwnedState;

    /** Secondary replica of the operator subtask state for faster, local recovery on TM. */
    private final OperatorSubtaskState taskLocalState;

    public OperatorSnapshotFinalizer(@Nonnull OperatorSnapshotFutures snapshotFutures)
            throws ExecutionException, InterruptedException {
        //多易教育: 这里run的是 OperatorSnapshotFutures 中的各种 RunnableFuture（如，keyedStateManagedFuture,keyedStateRawFuture等，就是对算子所使用的的各种状态进行快照处理）
        // 而这些 RunnableFuture，是 对 SnapshotStrategy对应各种backend实现中的asyncSnapshot()逻辑的封装（尤其是方法最后所返回的supplier.get）（以异步执行这些逻辑）
        // 在AbstractStreamOperator中由snapshot()的实现   //在AbstractUdfStreamOperator类中，也有 snapshotState()的实现，逻辑还不一样，有点诡异
        //   => 它里面调用了 stateHandler.snapshotState() , handler是 StreamOperatorStateHandler类
        //   => 进而调用 streamOperator.snapshotState(snapshotContext);
        //   => 只传入了context参数，那么实际上调用的是 AbstractUdfStreamOperator类的 snapshotState(snapshotContext)
        //   => 它里面用了 StreamingFunctionUtils#snapshotFunctionState()来执行snapshot
        //   => StreamingFunctionUtils.snapshotFunctionState,则执行正式的快照逻辑
        //      => 1. 先判断userFunction是否是CheckpointedFunction，
        //               如是，则调用了 userFunction.snapshotState() 对用户自定义的一些逻辑进行处理；
        //               如不是则userFunction压根没有这个方法
        //      => 2. 然后开始执行 算子状态、监控状态的 快照流程


        SnapshotResult<KeyedStateHandle> keyedManaged =
                FutureUtils.runIfNotDoneAndGet(snapshotFutures.getKeyedStateManagedFuture());

        SnapshotResult<KeyedStateHandle> keyedRaw =
                FutureUtils.runIfNotDoneAndGet(snapshotFutures.getKeyedStateRawFuture());

        SnapshotResult<OperatorStateHandle> operatorManaged =
                FutureUtils.runIfNotDoneAndGet(snapshotFutures.getOperatorStateManagedFuture());

        SnapshotResult<OperatorStateHandle> operatorRaw =
                FutureUtils.runIfNotDoneAndGet(snapshotFutures.getOperatorStateRawFuture());

        SnapshotResult<StateObjectCollection<InputChannelStateHandle>> inputChannel =
                snapshotFutures.getInputChannelStateFuture().get();

        SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>> resultSubpartition =
                snapshotFutures.getResultSubpartitionStateFuture().get();

        jobManagerOwnedState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                singletonOrEmpty(operatorManaged.getJobManagerOwnedSnapshot()))
                        .setRawOperatorState(
                                singletonOrEmpty(operatorRaw.getJobManagerOwnedSnapshot()))
                        .setManagedKeyedState(
                                singletonOrEmpty(keyedManaged.getJobManagerOwnedSnapshot()))
                        .setRawKeyedState(singletonOrEmpty(keyedRaw.getJobManagerOwnedSnapshot()))
                        .setInputChannelState(
                                emptyIfNull(inputChannel.getJobManagerOwnedSnapshot()))
                        .setResultSubpartitionState(
                                emptyIfNull(resultSubpartition.getJobManagerOwnedSnapshot()))
                        .build();

        taskLocalState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                singletonOrEmpty(operatorManaged.getTaskLocalSnapshot()))
                        .setRawOperatorState(singletonOrEmpty(operatorRaw.getTaskLocalSnapshot()))
                        .setManagedKeyedState(singletonOrEmpty(keyedManaged.getTaskLocalSnapshot()))
                        .setRawKeyedState(singletonOrEmpty(keyedRaw.getTaskLocalSnapshot()))
                        .setInputChannelState(emptyIfNull(inputChannel.getTaskLocalSnapshot()))
                        .setResultSubpartitionState(
                                emptyIfNull(resultSubpartition.getTaskLocalSnapshot()))
                        .build();
    }

    public OperatorSubtaskState getTaskLocalState() {
        return taskLocalState;
    }

    public OperatorSubtaskState getJobManagerOwnedState() {
        return jobManagerOwnedState;
    }
}
