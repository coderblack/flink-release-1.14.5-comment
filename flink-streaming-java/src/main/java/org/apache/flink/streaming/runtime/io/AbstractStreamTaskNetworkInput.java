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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for network-based StreamTaskInput where each channel has a designated {@link
 * RecordDeserializer} for spanning records. Specific implementation bind it to a specific {@link
 * RecordDeserializer}.
 */
public abstract class AbstractStreamTaskNetworkInput<
                T, R extends RecordDeserializer<DeserializationDelegate<StreamElement>>>
        implements StreamTaskInput<T> {
    protected final CheckpointedInputGate checkpointedInputGate;
    protected final DeserializationDelegate<StreamElement> deserializationDelegate;
    protected final TypeSerializer<T> inputSerializer;
    protected final Map<InputChannelInfo, R> recordDeserializers;
    protected final Map<InputChannelInfo, Integer> flattenedChannelIndices = new HashMap<>();
    /** Valve that controls how watermarks and watermark statuses are forwarded. */
    protected final StatusWatermarkValve statusWatermarkValve;

    protected final int inputIndex;
    private InputChannelInfo lastChannel = null;
    private R currentRecordDeserializer = null;

    public AbstractStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            Map<InputChannelInfo, R> recordDeserializers) {
        super();
        this.checkpointedInputGate = checkpointedInputGate;
        deserializationDelegate =
                new NonReusingDeserializationDelegate<>(
                        new StreamElementSerializer<>(inputSerializer));
        this.inputSerializer = inputSerializer;

        for (InputChannelInfo i : checkpointedInputGate.getChannelInfos()) {
            flattenedChannelIndices.put(i, flattenedChannelIndices.size());
        }

        this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
        this.inputIndex = inputIndex;
        this.recordDeserializers = checkNotNull(recordDeserializers);
    }

    //多易教育: 执行输入数据处理并输出结果的核心逻辑
    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {

        while (true) {
            // get the stream element from the deserializer
            //多易教育: 如果当前记录的反序列化器不为空，则先从反序列化器中获取到输入的流元素
            if (currentRecordDeserializer != null) {
                RecordDeserializer.DeserializationResult result;
                try {
                    result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                } catch (IOException e) {
                    throw new IOException(
                            String.format("Can't get next record for channel %s", lastChannel), e);
                }
                //多易教育:
                // q&a: 这里将deserializer置空，意思不明
                if (result.isBufferConsumed()) {
                    currentRecordDeserializer = null;
                }

                if (result.isFullRecord()) {
                    // 多易教育:  output:OneInputStreamTask$StreamTaskNetworkOutput
                    processElement(deserializationDelegate.getInstance(), output);
                    return DataInputStatus.MORE_AVAILABLE;
                }
            }

            //多易教育: 从输入口拉取一个 数据
            // checkpointedInputGate.pollNext()方法,会检测拉取到的是buffer还是event
            //  如果是buffer，直接返回
            //  如果是event，则根据event的类型做相应处理后，返回拉取到的event
            //  吐槽：变量方法等命名实在是太烂了，烂的一比！！！
            Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext(); // 多易教育:  CheckpointedInputGate
            if (bufferOrEvent.isPresent()) {
                // return to the mailbox after receiving a checkpoint barrier to avoid processing of
                // data after the barrier before checkpoint is performed for unaligned checkpoint
                // mode
                //多易教育-----------------
                // 收到 ck barrier 时马上回到 mailbox ，
                // 以避免在非对齐ck模式下出现 ck执行前就开始处理barrier后面的数据
                // q&a:这一段不知道什么鸟意思!!!
                //------------------

                //多易教育: 如果是数据buffer，则调用： processBuffer()
                if (bufferOrEvent.get().isBuffer()) {
                    //多易教育: 这里并没有实质处理动作，只是把channel中获取的buffer数据放入反序列化器，
                    // 以在下一次循环时，上面的处理逻辑能有数据处理
                    processBuffer(bufferOrEvent.get());
                }
                //多易教育: 如果是事件buffer，则调用：processEvent()
                // 此处的event处理只是对几个特定事件（输入结束等）进行状态获取了
                // 上面的 pollNext()拉取数据时就已经对checkpointBarrier做了处理
                //  吐槽：变量方法等命名实在是太烂了，烂的一比！！！
                else {
                    return processEvent(bufferOrEvent.get());
                }
            } else {
                if (checkpointedInputGate.isFinished()) {
                    checkState(
                            checkpointedInputGate.getAvailableFuture().isDone(),
                            "Finished BarrierHandler should be available");
                    return DataInputStatus.END_OF_INPUT;
                }
                return DataInputStatus.NOTHING_AVAILABLE;
            }
        }
    }

    private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {

        if (recordOrMark.isRecord()) {  //多易教育: 流数据处理
            output.emitRecord(recordOrMark.asRecord());
        } else if (recordOrMark.isWatermark()) {  //多易教育: watermark处理
            statusWatermarkValve.inputWatermark(
                    recordOrMark.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
        } else if (recordOrMark.isLatencyMarker()) {  //多易教育: 延迟标记处理
            output.emitLatencyMarker(recordOrMark.asLatencyMarker());
        } else if (recordOrMark.isWatermarkStatus()) {  //多易教育: watermark状态处理
            statusWatermarkValve.inputWatermarkStatus(
                    recordOrMark.asWatermarkStatus(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
        } else {
            throw new UnsupportedOperationException("Unknown type of StreamElement");
        }
    }

    protected DataInputStatus processEvent(BufferOrEvent bufferOrEvent) {
        // Event received
        //多易教育: AbstractEvent实现类由RuntimeEvent
        // RuntimeEvent下有
        // EndOfData、EndOfChannelStateEvent、
        // CheckpointBarrier、EndOfPartitionEvent等
        final AbstractEvent event = bufferOrEvent.getEvent();
        if (event.getClass() == EndOfData.class) {
            if (checkpointedInputGate.hasReceivedEndOfData()) {
                return DataInputStatus.END_OF_DATA;
            }
        } else if (event.getClass() == EndOfPartitionEvent.class) {
            // release the record deserializer immediately,
            // which is very valuable in case of bounded stream
            releaseDeserializer(bufferOrEvent.getChannelInfo());
            if (checkpointedInputGate.isFinished()) {
                return DataInputStatus.END_OF_INPUT;
            }
        } else if (event.getClass() == EndOfChannelStateEvent.class) {
            if (checkpointedInputGate.allChannelsRecovered()) {
                return DataInputStatus.END_OF_RECOVERY;
            }
        }
        return DataInputStatus.MORE_AVAILABLE;
    }

    protected void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
        lastChannel = bufferOrEvent.getChannelInfo();
        checkState(lastChannel != null);
        currentRecordDeserializer = getActiveSerializer(bufferOrEvent.getChannelInfo());
        checkState(
                currentRecordDeserializer != null,
                "currentRecordDeserializer has already been released");

        currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
    }

    protected R getActiveSerializer(InputChannelInfo channelInfo) {
        return recordDeserializers.get(channelInfo);
    }

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (currentRecordDeserializer != null) {
            return AVAILABLE;
        }
        return checkpointedInputGate.getAvailableFuture();
    }

    @Override
    public void close() throws IOException {
        // release the deserializers . this part should not ever fail
        for (InputChannelInfo channelInfo : new ArrayList<>(recordDeserializers.keySet())) {
            releaseDeserializer(channelInfo);
        }
    }

    protected void releaseDeserializer(InputChannelInfo channelInfo) {
        R deserializer = recordDeserializers.get(channelInfo);
        if (deserializer != null) {
            // recycle buffers and clear the deserializer.
            deserializer.clear();
            recordDeserializers.remove(channelInfo);
        }
    }
}
