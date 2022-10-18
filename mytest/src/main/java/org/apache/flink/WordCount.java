package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> words = s1
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                })
                .setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> pair = words.map(s -> Tuple2.of(s, 1))
                .setParallelism(1)
                .returns(new TypeHint<Tuple2<String, Integer>>() {});

        KeyedStream<Tuple2<String, Integer>, String> keyed = pair.keyBy(w -> w.f0);
        keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1).setParallelism(1)
                .print().setParallelism(1);

        env.execute();

    }
}
