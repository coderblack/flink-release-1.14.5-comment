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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This is
 * in contrast to implementations where records are written to a joint structure, from which the
 * subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 */
public abstract class BufferWritingResultPartition extends ResultPartition {

    /** The subpartitions of this partition. At least one. */
    protected final ResultSubpartition[] subpartitions;

    /**
     * For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be
     * null.
     */
    private final BufferBuilder[] unicastBufferBuilders; // TODO BY dps@51doit.cn : 对于单播模式，每个sub分区对应一个独立的BufferBuilder

    /** For broadcast mode, a single BufferBuilder is shared by all subpartitions. */
    private BufferBuilder broadcastBufferBuilder;

    private TimerGauge backPressuredTimeMsPerSecond = new TimerGauge();

    public BufferWritingResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                subpartitions.length,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.subpartitions = checkNotNull(subpartitions);
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];
    }

    @Override
    public void setup() throws IOException {
        super.setup();

        checkState(
                bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
                "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for"
                        + " this result partition.");
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }

    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }

        subpartitions[targetSubpartition].flush();
    }

    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);  // TODO BY dps@51doit.cn : 为新记录追加单播数据
        // TODO BY dps@51doit.cn : 只要record内容没有完全写入memorySegment，则循环不断写
        while (record.hasRemaining()) {
            // full buffer, partial record   满buffer，部分record
            finishUnicastBufferBuilder(targetSubpartition);  // TODO BY dps@51doit.cn : 完成bufferBuilder（更新输出字节计数器，输出缓存计数器，回收内存）
            buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition); // TODO BY dps@51doit.cn : 追加后续内容到单播数据
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishUnicastBufferBuilder(targetSubpartition);
        }

        // partial buffer, full record
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        BufferBuilder buffer = appendBroadcastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishBroadcastBufferBuilder();
            buffer = appendBroadcastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishBroadcastBufferBuilder();
        }

        // partial buffer, full record
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        checkInProduceState();
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        try (BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
            for (ResultSubpartition subpartition : subpartitions) {
                // Retain the buffer so that it can be recycled by each channel of targetPartition
                subpartition.add(eventBufferConsumer.copy(), 0);
            }
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        backPressuredTimeMsPerSecond = metrics.getBackPressuredTimePerSecond();
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
        checkState(!isReleased(), "Partition released.");

        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);

        LOG.debug("Created {}", readView);

        return readView;
    }

    @Override
    public void finish() throws IOException {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.finish();
        }

        super.finish();
    }

    @Override
    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            }
            // Catch this in order to ensure that release is called on all subpartitions
            catch (Throwable t) {
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        // We can not close these buffers in the release method because of the potential race
        // condition. This close method will be only called from the Task thread itself.
        if (broadcastBufferBuilder != null) {
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
        for (int i = 0; i < unicastBufferBuilders.length; ++i) {
            if (unicastBufferBuilders[i] != null) {
                unicastBufferBuilders[i].close();
                unicastBufferBuilders[i] = null;
            }
        }
        super.close();
    }

    private BufferBuilder appendUnicastDataForNewRecord(
            final ByteBuffer record, final int targetSubpartition) throws IOException {
        if (targetSubpartition < 0 || targetSubpartition > unicastBufferBuilders.length) {
            throw new ArrayIndexOutOfBoundsException(targetSubpartition);
        }
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];
        // TODO BY dps@51doit.cn : 如果目标分区对应的BufferBuilder为空，则申请一个新的BufferBuilder
        if (buffer == null) {
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);
            addToSubpartition(buffer, targetSubpartition, 0);
        }
        // TODO BY dps@51doit.cn : 追加和提交数据到bufferBuilder（底层即为写入memorySegment）
        buffer.appendAndCommit(record);

        return buffer;
    }

    private void addToSubpartition(BufferBuilder buffer, int targetSubpartition, int i)
            throws IOException {
        int desirableBufferSize =
                subpartitions[targetSubpartition].add(
                        buffer.createBufferConsumerFromBeginning(), i);

        if (desirableBufferSize > 0) {
            // !! If some of partial data has written already to this buffer, the result size can
            // not be less than written value.
            buffer.trim(desirableBufferSize);
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForNewRecord(final ByteBuffer record)
            throws IOException {
        BufferBuilder buffer = broadcastBufferBuilder;

        if (buffer == null) {
            buffer = requestNewBroadcastBufferBuilder();
            createBroadcastBufferConsumers(buffer, 0);
        }

        buffer.appendAndCommit(record);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes) throws IOException {
        final BufferBuilder buffer = requestNewBroadcastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
        createBroadcastBufferConsumers(buffer, partialRecordBytes);

        return buffer;
    }

    private void createBroadcastBufferConsumers(BufferBuilder buffer, int partialRecordBytes)
            throws IOException {
        try (final BufferConsumer consumer = buffer.createBufferConsumerFromBeginning()) {
            for (ResultSubpartition subpartition : subpartitions) {
                subpartition.add(consumer.copy(), partialRecordBytes);
            }
        }
    }

    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition)
            throws IOException {
        checkInProduceState();
        ensureUnicastMode();
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;

        return bufferBuilder;
    }

    private BufferBuilder requestNewBroadcastBufferBuilder() throws IOException {
        checkInProduceState();
        ensureBroadcastMode();

        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }

    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition)
            throws IOException {
        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition); // TODO BY dps@51doit.cn : 根据目标sub分区，申请memorySegment
        if (bufferBuilder != null) {
            return bufferBuilder;
        }
        // TODO BY dps@51doit.cn : 如果memorySegment申请失败，则开启背压计时
        backPressuredTimeMsPerSecond.markStart();
        try {  // TODO BY dps@51doit.cn : 再次申请memorySegment，并以阻塞方式申请（底层有一个ArrayDeque<MemorySegment>）
            bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
            backPressuredTimeMsPerSecond.markEnd();  // TODO BY dps@51doit.cn : 阻塞式申请成功，结束背压计时
            return bufferBuilder;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for buffer");
        }
    }

    private void finishUnicastBufferBuilder(int targetSubpartition) {
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        if (bufferBuilder != null) {
            numBytesOut.inc(bufferBuilder.finish()); // TODO BY dps@51doit.cn : 更新输出字节数计数器
            numBuffersOut.inc();// TODO BY dps@51doit.cn : 更新输出buffer数计数器
            unicastBufferBuilders[targetSubpartition] = null; // TODO BY dps@51doit.cn : 将目标sub分区的bufferBuilder位置为空
            bufferBuilder.close(); // TODO BY dps@51doit.cn : 关闭bufferBuilder（回收内存）
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int channelIndex = 0; channelIndex < numSubpartitions; channelIndex++) {
            finishUnicastBufferBuilder(channelIndex);
        }
    }

    private void finishBroadcastBufferBuilder() {
        if (broadcastBufferBuilder != null) {
            numBytesOut.inc(broadcastBufferBuilder.finish() * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
    }

    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();
    }

    private void ensureBroadcastMode() {
        finishUnicastBufferBuilders();
    }

    @VisibleForTesting
    public TimerGauge getBackPressuredTimeMsPerSecond() {
        return backPressuredTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }
}
