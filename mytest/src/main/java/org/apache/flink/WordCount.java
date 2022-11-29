package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("execution.checkpointing.unaligned",true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        /*env.getCheckpointConfig().setForceUnalignedCheckpoints(true);
        env.getCheckpointConfig().enableUnalignedCheckpoints();*/
        env.getCheckpointConfig().setCheckpointStorage("file:///home/hunter/ck");

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> words = s1.keyBy(s->s)
                .flatMap(new RichFlatMapFunction<String, String>() {
                    ListState<Person> p1;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        p1 = getRuntimeContext().getListState(new ListStateDescriptor<Person>(
                                "p1",
                                Person.class));
                    }

                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {

                        for(int i=0;i<10;i++){
                            p1.add(new Person("a","b",18));
                        }

                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                })
                .setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> pair = words.map(s -> Tuple2.of(s, 1))
                .setParallelism(1)
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });

        KeyedStream<Tuple2<String, Integer>, String> keyed = pair.keyBy(w -> w.f0);
        keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    ListState<Person> words1;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        words1 = getRuntimeContext().getListState(new ListStateDescriptor<Person>(
                                "words",
                                Person.class));
                    }

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        for(int i=0;i<1000;i++) {
                            words1.add(new Person(value.f0, value.f0, value.f1));
                        }

                        return value;
                    }
                }).setParallelism(4)
                .keyBy(tp -> tp.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1).setParallelism(3)
                .print().setParallelism(1);

        env.execute();

    }

    public static class Person{
        String id;
        String name;
        int age;

        public Person(String id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }
    }

}
