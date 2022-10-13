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

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AckAllUserRecordsProcessed;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.NewBufferSize;
import org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ResumeConsumption;
import org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Channel handler to initiate data transfers and dispatch backwards flowing task events. */
// TODO BY dps@51doit.cn : channel handler： 负责发起数据传输和回流task事件的分发
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

    private final ResultPartitionProvider partitionProvider;

    private final TaskEventPublisher taskEventPublisher;

    private final PartitionRequestQueue outboundQueue;

    PartitionRequestServerHandler(
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            PartitionRequestQueue outboundQueue) {

        this.partitionProvider = partitionProvider;
        this.taskEventPublisher = taskEventPublisher;
        this.outboundQueue = outboundQueue;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    // TODO BY dps@51doit.cn : 消息（数据或事件）处理逻辑，各种不同类型的消息处理中核心都是对outBoundQueue进行操作
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();

            // ----------------------------------------------------------------
            // Intermediate result partition requests  分区请求消息处理
            // ----------------------------------------------------------------
            if (msgClazz == PartitionRequest.class) {
                PartitionRequest request = (PartitionRequest) msg;

                LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

                try {
                    NetworkSequenceViewReader reader;  // TODO BY dps@51doit.cn : 创建CreditBasedSequenceNumberingViewReader
                    reader =
                            new CreditBasedSequenceNumberingViewReader(
                                    request.receiverId, request.credit, outboundQueue);
                    //TODO BY dps@51doit.cn : 通过 ResultPartitionProvider（实际上就是 ResultPartitionManager）创建 ResultSubpartitionView，并被reader持有为成员变量
                    // 在有可被消费的数据产生后，PartitionRequestQueue.notifyReaderNonEmpty 会被回调，进而在 netty channelPipeline 上触发一次 fireUserEventTriggered
                    // fireUserEventTriggered触发后，最终会导致reader被放入可用队列：enqueueAvailableReader((NetworkSequenceViewReader) msg);
                    reader.requestSubpartitionView(
                            partitionProvider, request.partitionId, request.queueIndex);
                    // TODO BY dps@51doit.cn : 通知outBoundQueue，创建了一个 NetworkSequenceViewReader
                    outboundQueue.notifyReaderCreated(reader);
                } catch (PartitionNotFoundException notFound) {
                    respondWithError(ctx, notFound, request.receiverId);
                }
            }
            // ----------------------------------------------------------------
            // Task events  task事件处理
            // ----------------------------------------------------------------
            else if (msgClazz == TaskEventRequest.class) {
                TaskEventRequest request = (TaskEventRequest) msg;
                // TODO BY dps@51doit.cn : 分派TaskEvent事件
                if (!taskEventPublisher.publish(request.partitionId, request.event)) {
                    respondWithError(
                            ctx,
                            new IllegalArgumentException("Task event receiver not found."),
                            request.receiverId);
                }
            } else if (msgClazz == CancelPartitionRequest.class) {  // TODO BY dps@51doit.cn : 分区请求取消处理
                CancelPartitionRequest request = (CancelPartitionRequest) msg;

                outboundQueue.cancel(request.receiverId);
            } else if (msgClazz == CloseRequest.class) {  // TODO BY dps@51doit.cn : 关闭请求取消处理
                outboundQueue.close();
            } else if (msgClazz == AddCredit.class) {  // TODO BY dps@51doit.cn : 增加信用值处理
                AddCredit request = (AddCredit) msg;

                outboundQueue.addCreditOrResumeConsumption(
                        request.receiverId, reader -> reader.addCredit(request.credit));
            } else if (msgClazz == ResumeConsumption.class) {  // TODO BY dps@51doit.cn : 恢复消费处理
                ResumeConsumption request = (ResumeConsumption) msg;

                outboundQueue.addCreditOrResumeConsumption(
                        request.receiverId, NetworkSequenceViewReader::resumeConsumption);
            } else if (msgClazz == AckAllUserRecordsProcessed.class) {  // TODO BY dps@51doit.cn : 确认所有用户数据处理完成
                AckAllUserRecordsProcessed request = (AckAllUserRecordsProcessed) msg;

                outboundQueue.acknowledgeAllRecordsProcessed(request.receiverId);
            } else if (msgClazz == NewBufferSize.class) {  // TODO BY dps@51doit.cn : bufferSize更新处理
                NewBufferSize request = (NewBufferSize) msg;

                outboundQueue.notifyNewBufferSize(request.receiverId, request.bufferSize);
            } else {
                LOG.warn("Received unexpected client request: {}", msg);
            }
        } catch (Throwable t) {
            respondWithError(ctx, t);
        }
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
    }

    private void respondWithError(
            ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
        LOG.debug("Responding with error: {}.", error.getClass());

        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
    }
}
