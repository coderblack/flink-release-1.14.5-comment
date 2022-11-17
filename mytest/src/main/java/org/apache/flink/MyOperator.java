package org.apache.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;

public class MyOperator extends RichMapFunction<String,String> implements CheckpointedFunction {

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public String map(String value) throws Exception {
        return null;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {



    }
}
