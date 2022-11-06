/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;

/** A regular non finished on restore {@link OperatorChain}. */
@Internal
public class RegularOperatorChain<OUT, OP extends StreamOperator<OUT>>
        extends OperatorChain<OUT, OP> {

    private static final Logger LOG = LoggerFactory.getLogger(RegularOperatorChain.class);

    public RegularOperatorChain(
            StreamTask<OUT, OP> containingTask,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate) {
        super(containingTask, recordWriterDelegate);
    }

    @VisibleForTesting
    RegularOperatorChain(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            RecordWriterOutput<?>[] streamOutputs,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput,
            StreamOperatorWrapper<OUT, OP> mainOperatorWrapper) {
        super(allOperatorWrappers, streamOutputs, mainOperatorOutput, mainOperatorWrapper);
    }

    @Override
    public boolean isTaskDeployedAsFinished() {
        return false;
    }

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        operatorEventDispatcher.dispatchEventToHandlers(operator, event);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // go forward through the operator chain and tell each operator
        // to prepare the checkpoint
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators()) {
            if (!operatorWrapper.isClosed()) {
                operatorWrapper.getStreamOperator().prepareSnapshotPreBarrier(checkpointId);
            }
        }
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (mainOperatorWrapper != null) {
            mainOperatorWrapper.endOperatorInput(inputId);
        }
    }

    //多易教育: 对chain中的每个算子，逐一初始化算子状态，并open算子
    // 初始化算子状态时，会为算子构造一个 StreamOperatorStateHandler
    @Override
    public void initializeStateAndOpenOperators(
            StreamTaskStateInitializer streamTaskStateInitializer) throws Exception {
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            StreamOperator<?> operator = operatorWrapper.getStreamOperator();
            operator.initializeState(streamTaskStateInitializer);
            operator.open(); //多易教育: 调用算子函数的open
        }
    }

    @Override
    public void finishOperators(StreamTaskActionExecutor actionExecutor) throws Exception {
        if (firstOperatorWrapper != null) {
            firstOperatorWrapper.finish(actionExecutor);
        }
    }

    @Override
    public void closeAllOperators() throws Exception {
        super.closeAllOperators();
        Exception closingException = null;
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            try {
                operatorWrapper.close();
            } catch (Exception e) {
                closingException = firstOrSuppressed(e, closingException);
            }
        }
        if (closingException != null) {
            throw closingException;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Nullable
    StreamOperator<?> getTailOperator() {
        return (tailOperatorWrapper == null) ? null : tailOperatorWrapper.getStreamOperator();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        Exception previousException = null;
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            try {
                operatorWrapper.notifyCheckpointComplete(checkpointId);
            } catch (Exception e) {
                previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
            }
        }
        ExceptionUtils.tryRethrowException(previousException);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        Exception previousException = null;
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            try {
                operatorWrapper.getStreamOperator().notifyCheckpointAborted(checkpointId);
            } catch (Exception e) {
                previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
            }
        }
        ExceptionUtils.tryRethrowException(previousException);
    }

    //多易教育: 调用者  SubtaskCheckpointCoordinatorImpl#takeSnapshotSync
    @Override
    public void snapshotState(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception {
        //多易教育: 遍历算子链中的每一个算子，逐个执行状态snapshot
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            if (!operatorWrapper.isClosed()) {
                operatorSnapshotsInProgress.put(
                        operatorWrapper.getStreamOperator().getOperatorID(),
                        //多易教育: 调用checkpoint的中间环节
                        buildOperatorSnapshotFutures(
                                checkpointMetaData,
                                checkpointOptions,
                                operatorWrapper.getStreamOperator(),
                                isRunning,
                                channelStateWriteResult,
                                storage));
            }
        }
    }

    //多易教育: 内部方法，调用者：本类#snapshotState
    private OperatorSnapshotFutures buildOperatorSnapshotFutures(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            StreamOperator<?> op,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception {
        //多易教育: 触发operator执行cp，获得进度信息future
        OperatorSnapshotFutures snapshotInProgress =
                checkpointStreamOperator(
                        op, checkpointMetaData, checkpointOptions, storage, isRunning);
        //多易教育: 如果是头部算子，还需要对inputChannel的缓存数据进行处理
        if (op == getMainOperator()) {
            snapshotInProgress.setInputChannelStateFuture(
                    channelStateWriteResult
                            .getInputChannelStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of));
        }
        //多易教育: 如果是尾部算子，还需要对输出数据进行处理
        if (op == getTailOperator()) {
            snapshotInProgress.setResultSubpartitionStateFuture(
                    channelStateWriteResult
                            .getResultSubpartitionStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of));
        }
        return snapshotInProgress;
    }

    //多易教育: 调用者在上面
    private static OperatorSnapshotFutures checkpointStreamOperator(
            StreamOperator<?> op,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation,
            Supplier<Boolean> isRunning)
            throws Exception {
        try {
            //多易教育: 调用算子operator的snapshotState,
            // 算子内会用 StreamOperatorStateHandler.snapshotState(context)，
            // 而handler中，会有snapshot的完整步骤，包含执行用户函数中的snapshotState方法的步骤
            return op.snapshotState(
                    checkpointMetaData.getCheckpointId(),
                    checkpointMetaData.getTimestamp(),
                    checkpointOptions,
                    storageLocation);
        } catch (Exception ex) {
            if (isRunning.get()) {
                LOG.info(ex.getMessage(), ex);
            }
            throw ex;
        }
    }
}
