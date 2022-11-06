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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * A class to execute a {@link SnapshotStrategy}. It can execute a strategy either synchronously or
 * asynchronously. It takes care of common logging and resource cleaning.
 *
 * @param <T> type of the snapshot result.
 */
public final class SnapshotStrategyRunner<T extends StateObject, SR extends SnapshotResources> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotStrategyRunner.class);

    private static final String LOG_SYNC_COMPLETED_TEMPLATE =
            "{} ({}, synchronous part) in thread {} took {} ms.";
    private static final String LOG_ASYNC_COMPLETED_TEMPLATE =
            "{} ({}, asynchronous part) in thread {} took {} ms.";

    /**
     * Descriptive name of the snapshot strategy that will appear in the log outputs and {@link
     * #toString()}.
     */
    @Nonnull private final String description;

    @Nonnull private final SnapshotStrategy<T, SR> snapshotStrategy;
    @Nonnull private final CloseableRegistry cancelStreamRegistry;

    @Nonnull private final SnapshotExecutionType executionType;

    public SnapshotStrategyRunner(
            @Nonnull String description,
            @Nonnull SnapshotStrategy<T, SR> snapshotStrategy,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            @Nonnull SnapshotExecutionType executionType) {
        this.description = description;
        this.snapshotStrategy = snapshotStrategy;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.executionType = executionType;
    }

    //多易教育: 状态快照，调用者为 DefaultOperatorStateBackend#snapshot
    @Nonnull
    public final RunnableFuture<SnapshotResult<T>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        long startTime = System.currentTimeMillis();
        SR snapshotResources = snapshotStrategy.syncPrepareResources(checkpointId);
        logCompletedInternal(LOG_SYNC_COMPLETED_TEMPLATE, streamFactory, startTime);
        //多易教育: 构造SnapshotResultSupplier，supplier有一个核心get方法在 赋值逻辑<snapshotStrategy.asyncSnapshot()>中匿名实现
        SnapshotStrategy.SnapshotResultSupplier<T> asyncSnapshot =
                snapshotStrategy.asyncSnapshot(
                        snapshotResources,
                        checkpointId,
                        timestamp,
                        streamFactory,
                        checkpointOptions);

        //多易教育: 封装异步快照的执行逻辑
        FutureTask<SnapshotResult<T>> asyncSnapshotTask =
                new AsyncSnapshotCallable<SnapshotResult<T>>() {
                    @Override
                    protected SnapshotResult<T> callInternal() throws Exception {
                        //多易教育:snapshot最终输出逻辑------------------------
                        // 此处调用的就是上面构造的 SnapshotResultSupplier的get方法
                        // 而这个supplier是在 HeapSnapShotStrategy#asyncSnapshot()方法中匿名实现的
                        // -----------------------------------------------------------
                        return asyncSnapshot.get(snapshotCloseableRegistry);
                    }

                    @Override
                    protected void cleanupProvidedResources() {
                        if (snapshotResources != null) {
                            snapshotResources.release();
                        }
                    }

                    @Override
                    protected void logAsyncSnapshotComplete(long startTime) {
                        logCompletedInternal(
                                LOG_ASYNC_COMPLETED_TEMPLATE, streamFactory, startTime);
                    }
                }.toAsyncSnapshotFutureTask(cancelStreamRegistry);
        //多易教育: 如果是同步ck策略，则马上执行
        if (executionType == SnapshotExecutionType.SYNCHRONOUS) {
            //多易教育: 执行异步快照的task逻辑
            // 这里是调用的FutureTask.run()，因而是在一个单独的线程中异步执行的
            // 如果触发ck的源头是每个operatorChain中各自触发，那么意味着每个chain其实都是各自开启线程来进行snapshot的
            asyncSnapshotTask.run();
        }
        //多易教育: 如果不是同步策略，则返回给调用层取异步执行
        return asyncSnapshotTask;
    }

    private void logCompletedInternal(
            @Nonnull String template, @Nonnull Object checkpointOutDescription, long startTime) {

        long duration = (System.currentTimeMillis() - startTime);

        LOG.debug(
                template, description, checkpointOutDescription, Thread.currentThread(), duration);
    }

    @Override
    public String toString() {
        return "SnapshotStrategy {" + description + "}";
    }
}
