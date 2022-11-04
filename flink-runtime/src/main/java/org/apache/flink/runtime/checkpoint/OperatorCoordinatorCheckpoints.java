/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * All the logic related to taking checkpoints of the {@link OperatorCoordinator}s.
 *
 * <p>NOTE: This class has a simplified error handling logic. If one of the several coordinator
 * checkpoints fail, no cleanup is triggered for the other concurrent ones. That is okay, since they
 * all produce just byte[] as the result. We have to change that once we allow then to create
 * external resources that actually need to be cleaned up.
 */
final class OperatorCoordinatorCheckpoints {

    public static CompletableFuture<CoordinatorSnapshot> triggerCoordinatorCheckpoint(
            final OperatorCoordinatorCheckpointContext coordinatorContext, final long checkpointId)
            throws Exception {
        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        //多易教育: 调用OperatorCoordinatorHolder来执行checkpointCoordinator()，并传入一个future来获取异步执行结果
        // 底层所返回的future结果是coordinator的snapshot后的状态的序列化字节数组
        coordinatorContext.checkpointCoordinator(checkpointId, checkpointFuture);
        //多易教育: 将返回的状态序列化字节数组，转成 CoordinatorSnapshot 对象封装
        return checkpointFuture.thenApply(
                (state) ->
                        new CoordinatorSnapshot(
                                coordinatorContext,
                                new ByteStreamStateHandle(
                                        coordinatorContext.operatorId().toString(), state)));
    }

    public static CompletableFuture<AllCoordinatorSnapshots> triggerAllCoordinatorCheckpoints(
            final Collection<OperatorCoordinatorCheckpointContext> coordinators,
            final long checkpointId)
            throws Exception {
        final Collection<CompletableFuture<CoordinatorSnapshot>> individualSnapshots =
                new ArrayList<>(coordinators.size());

        for (final OperatorCoordinatorCheckpointContext coordinator : coordinators) {
            //多易教育: 逐个遍历，触发异步cp，并得到future（
            // Future的内涵是一个 CoordinatorSnapshot，而CoordinatorSnapshot中则含有底层所返回的coordinator状态snapshot数据序列化字节）
            final CompletableFuture<CoordinatorSnapshot> checkpointFuture =
                    triggerCoordinatorCheckpoint(coordinator, checkpointId);
            //多易教育: 将单个coordinator的cp结果future放入 集合
            individualSnapshots.add(checkpointFuture);
        }
        //多易教育: 最后，将单个snapshot数据future的集合，构建成一个整体封装对象的future返回
        return FutureUtils.combineAll(individualSnapshots).thenApply(AllCoordinatorSnapshots::new);
    }

    public static CompletableFuture<Void> triggerAndAcknowledgeAllCoordinatorCheckpoints(
            final Collection<OperatorCoordinatorCheckpointContext> coordinators,
            final PendingCheckpoint checkpoint,
            final Executor acknowledgeExecutor)
            throws Exception {
        //多易教育: 触发cp，
        // 这里的底层其实是对各个coordinator进行逐一snapshot，并序列化snapshot的数据，
        // 并将各snapshot数据的future，转成一个整体的Future<AllCoordinatorSnapshots> 返回
        final CompletableFuture<AllCoordinatorSnapshots> snapshots =
                triggerAllCoordinatorCheckpoints(coordinators, checkpoint.getCheckpointId());
        //多易教育: 确认所有coordinator的snapshot状态
        //   即，当上面步骤成功完成后，则将各snapshot的结果，逐个遍历，放入对应的OperatorId对应的 OperatorState.coordinatorState中去
        return snapshots.thenAcceptAsync(
                (allSnapshots) -> {
                    try {
                        acknowledgeAllCoordinators(checkpoint, allSnapshots.snapshots);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                acknowledgeExecutor);
    }

    public static CompletableFuture<Void>
            triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                    final Collection<OperatorCoordinatorCheckpointContext> coordinators,
                    final PendingCheckpoint checkpoint,
                    final Executor acknowledgeExecutor)
                    throws CompletionException {
        try {
            return triggerAndAcknowledgeAllCoordinatorCheckpoints(
                    coordinators, checkpoint, acknowledgeExecutor);
        } catch (Exception e) {
            throw new CompletionException(e);
        }
    }

    // ------------------------------------------------------------------------

    private static void acknowledgeAllCoordinators(
            PendingCheckpoint checkpoint, Collection<CoordinatorSnapshot> snapshots)
            throws CheckpointException {
        for (final CoordinatorSnapshot snapshot : snapshots) {
            final PendingCheckpoint.TaskAcknowledgeResult result =
                    checkpoint.acknowledgeCoordinatorState(snapshot.coordinator, snapshot.state);

            if (result != PendingCheckpoint.TaskAcknowledgeResult.SUCCESS) {
                final String errorMessage =
                        "Coordinator state not acknowledged successfully: " + result;
                final Throwable error =
                        checkpoint.isDisposed() ? checkpoint.getFailureCause() : null;

                CheckpointFailureReason reason = CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE;
                if (error != null) {
                    final Optional<IOException> ioExceptionOptional =
                            ExceptionUtils.findThrowable(error, IOException.class);
                    if (ioExceptionOptional.isPresent()) {
                        reason = CheckpointFailureReason.IO_EXCEPTION;
                    }

                    throw new CheckpointException(errorMessage, reason, error);
                } else {
                    throw new CheckpointException(errorMessage, reason);
                }
            }
        }
    }

    // ------------------------------------------------------------------------

    static final class AllCoordinatorSnapshots {

        private final Collection<CoordinatorSnapshot> snapshots;

        AllCoordinatorSnapshots(Collection<CoordinatorSnapshot> snapshots) {
            this.snapshots = snapshots;
        }

        public Iterable<CoordinatorSnapshot> snapshots() {
            return snapshots;
        }
    }

    static final class CoordinatorSnapshot {

        final OperatorInfo coordinator;
        final ByteStreamStateHandle state;

        CoordinatorSnapshot(OperatorInfo coordinator, ByteStreamStateHandle state) {
            this.coordinator = coordinator;
            this.state = state;
        }
    }
}
