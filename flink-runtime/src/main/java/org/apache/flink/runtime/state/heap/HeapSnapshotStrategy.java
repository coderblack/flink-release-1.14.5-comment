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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createDuplicatingStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createSimpleStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult;

/** A strategy how to perform a snapshot of a {@link HeapKeyedStateBackend}. */
class HeapSnapshotStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, HeapSnapshotResources<K>> {

    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;
    private final LocalRecoveryConfig localRecoveryConfig;
    private final KeyGroupRange keyGroupRange;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final int totalKeyGroups;

    HeapSnapshotStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            KeyGroupRange keyGroupRange,
            StateSerializerProvider<K> keySerializerProvider,
            int totalKeyGroups) {
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.localRecoveryConfig = localRecoveryConfig;
        this.keyGroupRange = keyGroupRange;
        this.keySerializerProvider = keySerializerProvider;
        this.totalKeyGroups = totalKeyGroups;
    }

    @Override
    public HeapSnapshotResources<K> syncPrepareResources(long checkpointId) {
        return HeapSnapshotResources.create(
                registeredKVStates,
                registeredPQStates,
                keyGroupCompressionDecorator,
                keyGroupRange,
                getKeySerializer(),
                totalKeyGroups);
    }

    //多易教育: 调用者为 ： SnapshotStrategyRunner#snapshot
    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            HeapSnapshotResources<K> syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        List<StateMetaInfoSnapshot> metaInfoSnapshots = syncPartResource.getMetaInfoSnapshots();
        if (metaInfoSnapshots.isEmpty()) {
            return snapshotCloseableRegistry -> SnapshotResult.empty();
        }

        final KeyedBackendSerializationProxy<K> serializationProxy =
                new KeyedBackendSerializationProxy<>(
                        // TODO: this code assumes that writing a serializer is threadsafe, we
                        // should support to
                        // get a serialized form already at state registration time in the future
                        syncPartResource.getKeySerializer(),
                        metaInfoSnapshots,
                        !Objects.equals(
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                keyGroupCompressionDecorator));

        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier =
                        localRecoveryConfig.isLocalRecoveryEnabled() //多易教育: 是否使用本地恢复
                                        && !checkpointOptions.getCheckpointType().isSavepoint()
                                ? () ->
                                        createDuplicatingStream(  //多易教育: 本地恢复并且当前不是savepoint，创建复制流
                                                checkpointId,
                                                CheckpointedStateScope.EXCLUSIVE,
                                                streamFactory,
                                                localRecoveryConfig
                                                        .getLocalStateDirectoryProvider())
                                : () ->
                                        createSimpleStream( //多易教育: 非本地恢复，或者是savepoint，创 建简单流
                                                CheckpointedStateScope.EXCLUSIVE, streamFactory);

        //多易教育: 匿名实现，实现了SnapshotResultSupplier的get()方法
        // 内部逻辑就是snapshot的数据写出逻辑
        return (snapshotCloseableRegistry) -> {
            final Map<StateUID, Integer> stateNamesToId = syncPartResource.getStateNamesToId();
            final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
                    syncPartResource.getCowStateStableSnapshots();
            final CheckpointStreamWithResultProvider streamWithResultProvider =
                    checkpointStreamSupplier.get();  //多易教育: supplier的匿名实现就在楼上

            snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);
            //多易教育: 获取输出流 , debug信息 => basePath = "file:/home/hunter/ck/99.../chk-3
            final CheckpointStreamFactory.CheckpointStateOutputStream localStream =
                    streamWithResultProvider.getCheckpointOutputStream();

            // 多易教育：测试输出
            System.out.println("在HeapSnapshotStrategy#asyncSnapshot(supplier)中，当前线程为： " + Thread.currentThread()+",线程号：" +Thread.currentThread().getId());
            System.out.println("在HeapSnapshotStrategy#asyncSnapshot(supplier)中，localStream为： " + localStream + "," + localStream.hashCode());

            //多易教育: 装饰者模式，以加入一些公共的附加逻辑
            final DataOutputViewStreamWrapper outView =
                    new DataOutputViewStreamWrapper(localStream);
            //多易教育: 写出key的序列化类class信息，及元数据
            serializationProxy.write(outView);

            //多易教育： ------------------
            // 写出各keyGroup的state数据内容
            // --------------------------

            //多易教育: keyGroupRange:{startKeyGroup=0,endKeyGroup=31}
            final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];
            for (int keyGroupPos = 0;
                    keyGroupPos < keyGroupRange.getNumberOfKeyGroups();
                    ++keyGroupPos) {
                int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
                keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
                outView.writeInt(keyGroupId);

                for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
                        cowStateStableSnapshots.entrySet()) {
                    StateSnapshot.StateKeyGroupWriter partitionedSnapshot =
                            stateSnapshot.getValue().getKeyGroupWriter();
                    try (OutputStream kgCompressionOut =
                            keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
                        DataOutputViewStreamWrapper kgCompressionView =
                                new DataOutputViewStreamWrapper(kgCompressionOut);
                        kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
                        partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
                    } // this will just close the outer compression stream
                }
            }

            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                KeyGroupRangeOffsets kgOffs =
                        new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
                SnapshotResult<StreamStateHandle> result =
                        streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                return toKeyedStateHandleSnapshotResult(result, kgOffs, KeyGroupsStateHandle::new);
            } else {
                throw new IOException("Stream already unregistered.");
            }
        };
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializerProvider.currentSchemaSerializer();
    }
}
