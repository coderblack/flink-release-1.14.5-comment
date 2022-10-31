package org.apache.flink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class MyTriggerRestoreHook implements MasterTriggerRestoreHook {
    @Override
    public String getIdentifier() {
        return null;
    }

    @Nullable
    @Override
    public CompletableFuture triggerCheckpoint(
            long checkpointId,
            long timestamp,
            Executor executor) throws Exception {
        return null;
    }

    @Override
    public void restoreCheckpoint(
            long checkpointId,
            @Nullable Object checkpointData) throws Exception {

    }

    @Nullable
    @Override
    public SimpleVersionedSerializer createCheckpointDataSerializer() {
        return null;
    }
}
