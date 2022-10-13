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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound handler about
 * non-emptiness, similar to the {@link LocalInputChannel}.
 */
class CreditBasedSequenceNumberingViewReader
        implements BufferAvailabilityListener, NetworkSequenceViewReader {  // TODO BY dps@51doit.cn : 实现了数据可用监听器

    private final Object requestLock = new Object();

    private final InputChannelID receiverId;

    //TODO BY dps@51doit.cn :
    // PartitionRequestQueue 负责将 ResultSubpartition 中的数据通过网络发送给 RemoteInputChannel。
    // 在 PartitionRequestQueue 中保存了所有的 NetworkSequenceViewReader 和 InputChannelID 之间的映射关系，
    // 以及一个 ArrayDeque<NetworkSequenceViewReader> availableReaders 队列。
    // 当一个 NetworkSequenceViewReader 中有数据可以被消费时，就会被加入到 availableReaders 队列中
    private final PartitionRequestQueue requestQueue;

    private final int initialCredit;

    // TODO BY dps@51doit.cn : 消费 ResultSubpartition 的数据，并在 ResultSubpartition 有数据可用时获得通知
    private volatile ResultSubpartitionView subpartitionView;

    /**
     * The status indicating whether this reader is already enqueued in the pipeline for
     * transferring data or not.
     *
     * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
     * thread only since there is no synchronisation.
     */
    private boolean isRegisteredAsAvailable = false;

    /** The number of available buffers for holding data on the consumer side. */
    // TODO BY dps@51doit.cn : 消费端还能够容纳的buffer的数量，也就是允许生产端发送的buffer的数量
    private int numCreditsAvailable;

    CreditBasedSequenceNumberingViewReader(
            InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {
        checkArgument(initialCredit >= 0, "Must be non-negative.");

        this.receiverId = receiverId;
        this.initialCredit = initialCredit;
        this.numCreditsAvailable = initialCredit;
        this.requestQueue = requestQueue;
    }

    // TODO BY dps@51doit.cn : 创建一个 ResultSubpartitionView，用于读取数据，并在有数据可用时获得通知
    @Override
    public void requestSubpartitionView(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            int subPartitionIndex)
            throws IOException {

        synchronized (requestLock) {
            if (subpartitionView == null) {
                // This call can trigger a notification we have to
                // schedule a separate task at the event loop that will
                // start consuming this. Otherwise the reference to the
                // view cannot be available in getNextBuffer().
                this.subpartitionView =  // TODO BY dps@51doit.cn : 创建ResultSubPartitionView
                        partitionProvider.createSubpartitionView(
                                resultPartitionId, subPartitionIndex, this);
            } else {
                throw new IllegalStateException("Subpartition already requested");
            }
        }
        //TODO BY dps@51doit.cn : 通知已有数据可用
        // 通知中的逻辑：触发一个用户自定义事件
        //  RequestQueue.notifyReaderNonEmpty(this);
        //       ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
        //  PartitionRequestQueue#userEventTriggered()
        //      if (msg instanceof NetworkSequenceViewReader) {
        //          enqueueAvailableReader((NetworkSequenceViewReader) msg);
        notifyDataAvailable();
    }

    @Override
    public void addCredit(int creditDeltas) {
        numCreditsAvailable += creditDeltas;
    }

    @Override
    public void resumeConsumption() {
        if (initialCredit == 0) {
            // reset available credit if no exclusive buffer is available at the
            // consumer side for all floating buffers must have been released
            numCreditsAvailable = 0;
        }
        subpartitionView.resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed() {
        subpartitionView.acknowledgeAllDataProcessed();
    }

    @Override
    public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
        this.isRegisteredAsAvailable = isRegisteredAvailable;
    }

    @Override
    public boolean isRegisteredAsAvailable() {
        return isRegisteredAsAvailable;
    }

    /**
     * Returns true only if the next buffer is an event or the reader has both available credits and
     * buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getNextDataType(BufferAndBacklog)}, such
     *     that {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     */
    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog() {
        return subpartitionView.getAvailabilityAndBacklog(numCreditsAvailable);
    }

    /**
     * Returns the {@link org.apache.flink.runtime.io.network.buffer.Buffer.DataType} of the next
     * buffer in line.
     *
     * <p>Returns the next data type only if the next buffer is an event or the reader has both
     * available credits and buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getAvailabilityAndBacklog()}, such that
     *     {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     * @param bufferAndBacklog current buffer and backlog including information about the next
     *     buffer
     * @return the next data type if the next buffer can be pulled immediately or {@link
     *     Buffer.DataType#NONE}
     */
    private Buffer.DataType getNextDataType(BufferAndBacklog bufferAndBacklog) {
        final Buffer.DataType nextDataType = bufferAndBacklog.getNextDataType();
        if (numCreditsAvailable > 0 || nextDataType.isEvent()) {
            return nextDataType;
        }
        return Buffer.DataType.NONE;
    }

    @Override
    public InputChannelID getReceiverId() {
        return receiverId;
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        subpartitionView.notifyNewBufferSize(newBufferSize);
    }

    @VisibleForTesting
    int getNumCreditsAvailable() {
        return numCreditsAvailable;
    }

    @VisibleForTesting
    ResultSubpartitionView.AvailabilityWithBacklog hasBuffersAvailable() {
        return subpartitionView.getAvailabilityAndBacklog(Integer.MAX_VALUE);
    }

    // TODO BY dps@51doit.cn : 读取数据
    @Nullable
    @Override
    public BufferAndAvailability getNextBuffer() throws IOException {
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        if (next != null) {
            if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {  // TODO BY dps@51doit.cn : 要发送一个buffer，对应的 numCreditsAvailable 要减 1
                throw new IllegalStateException("no credit available");
            }

            final Buffer.DataType nextDataType = getNextDataType(next);
            return new BufferAndAvailability(
                    next.buffer(), nextDataType, next.buffersInBacklog(), next.getSequenceNumber());
        } else {
            return null;
        }
    }

    @Override
    public boolean needAnnounceBacklog() {
        return initialCredit == 0 && numCreditsAvailable == 0;
    }

    @Override
    public boolean isReleased() {
        return subpartitionView.isReleased();
    }

    @Override
    public Throwable getFailureCause() {
        return subpartitionView.getFailureCause();
    }

    @Override
    public void releaseAllResources() throws IOException {
        subpartitionView.releaseAllResources();
    }

    //TODO BY dps@51doit.cn : 监听器接口方法，在 ResultSubpartition 中有数据时会回调该方法
    // 告知 PartitionRequestQueue 当前 ViewReader 有数据可读
    @Override
    public void notifyDataAvailable() {
        //TODO BY dps@51doit.cn : 通知中的逻辑：触发一个用户自定义事件
        // ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
        requestQueue.notifyReaderNonEmpty(this);
    }

    @Override
    public void notifyPriorityEvent(int prioritySequenceNumber) {
        notifyDataAvailable();
    }

    @Override
    public String toString() {
        return "CreditBasedSequenceNumberingViewReader{"
                + "requestLock="
                + requestLock
                + ", receiverId="
                + receiverId
                + ", numCreditsAvailable="
                + numCreditsAvailable
                + ", isRegisteredAsAvailable="
                + isRegisteredAsAvailable
                + '}';
    }
}
