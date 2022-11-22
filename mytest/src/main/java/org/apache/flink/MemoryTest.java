package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class MemoryTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.memory.task.heap.size", "5M");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                conf);

        //env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///home/hunter/cp/");
        env.setParallelism(1);

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);

        s1.map(new MapFunction<String, String>() {

            List<byte[]> lst = new ArrayList<>();

            @Override
            public String map(String value) throws Exception {
                /*byte[] bts = new byte[1024];
                lst.add(bts);*/
                return value;
            }
        }).print();

        env.execute();

    }
}
