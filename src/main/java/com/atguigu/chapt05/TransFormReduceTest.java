package com.atguigu.chapt05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TransFormReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        SingleOutputStreamOperator<Tuple2<String, Long>> map = stream.map(data -> Tuple2.of(data.user, 1L)).returns(new TypeHint<Tuple2<String, Long>>() {
            @Override
            public TypeInformation<Tuple2<String, Long>> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = map.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = tuple2StringKeyedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> reduce1 = reduce.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return stringLongTuple2.f1 > t1.f1 ? stringLongTuple2 : t1;
            }
        });
        reduce1.print("reduce: ");
        env.execute();
    }
}
