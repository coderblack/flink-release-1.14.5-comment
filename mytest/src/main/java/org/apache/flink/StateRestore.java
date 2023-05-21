package org.apache.flink;

import org.apache.commons.lang3.RandomUtils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateRestore {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt");
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9999);
        ds.keyBy(s -> 1)
                .map(new RichMapFunction<String, String>() {
                    ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                                "v",
                                String.class));

                    }

                    @Override
                    public String map(String value) throws Exception {

                        String old = state.value();
                        if(old == null ){
                            state.update(value);
                        }else{
                            state.update(old + value);
                        }

                        if(RandomUtils.nextInt(1,6)%5 == 0) throw new RuntimeException("ex---------------------");

                        return state.value();
                    }
                }).print();


        env.execute();
    }

}
