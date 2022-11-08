package org.apache.flink;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SnapshotStateTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.storage.fs.memory-threshold","0kb");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///home/hunter/ck");
        // ---  chain 1-----------
        DataStreamSource<String> chain1 = env.addSource(Utils.getSource()).setParallelism(1);
        KeyedStream<String, String> keyed = chain1.keyBy(s -> s);

        // ---  chain 2-----------
        SingleOutputStreamOperator<String> chain2 = keyed
                .map(new RichMapFunction<String, String>() {
                    ListState<String> l1;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        l1 = getRuntimeContext().getListState(new ListStateDescriptor<String>(
                                "l1",
                                String.class));
                    }

                    @Override
                    public String map(String value) throws Exception {
                        l1.add(value);

                        StringBuilder sb = new StringBuilder();
                        l1.get().forEach(sb::append);

                        return value;
                    }
                })
                .setParallelism(2);


        // ---  chain 3-----------
        chain2.keyBy(s->s)
                .map(new RichMapFunction<String, String>() {
                    ListState<String> l2;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        l2 = getRuntimeContext().getListState(new ListStateDescriptor<String>(
                                "l3",
                                String.class));
                    }

                    @Override
                    public String map(String value) throws Exception {
                        l2.add(value);
                        StringBuilder sb = new StringBuilder();
                        l2.get().forEach(sb::append);

                        return value;
                    }
                }).setParallelism(1)
                .print().setParallelism(1);

        env.execute();

    }
}
