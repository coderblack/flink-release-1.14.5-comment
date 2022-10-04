package org.apache.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);

        s1.map(s -> s + 10)
                .print();
        env.execute();

    }
}
