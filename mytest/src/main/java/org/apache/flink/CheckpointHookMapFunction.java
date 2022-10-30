package org.apache.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;

public class CheckpointHookMapFunction extends RichMapFunction<String,String> implements WithMasterCheckpointHook {
    @Override
    public String map(String value) throws Exception {
        return null;
    }

    @Override
    public MasterTriggerRestoreHook createMasterTriggerRestoreHook() {
        return null;
    }
}
