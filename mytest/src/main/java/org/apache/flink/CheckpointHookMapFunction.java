package org.apache.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;

public class CheckpointHookMapFunction extends RichMapFunction<String,String> implements WithMasterCheckpointHook, CheckpointedFunction {
    @Override
    public String map(String value) throws Exception {
        return null;
    }

    @Override
    public MasterTriggerRestoreHook createMasterTriggerRestoreHook() {
        return null;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {


    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        context.getOperatorStateStore();

    }
}
