package com.atguigu.chapter06.reduceaggregate;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //需要设置时间语义


        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));
        SingleOutputStreamOperator<Event> stream1 = stream.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Event>forBoundedOutOfOrderness(Duration.ofSeconds(1L)).
                withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                          @Override
                                          public long extractTimestamp(Event event, long l) {
                                              return event.timestamp;
                                          }
                                      }
                ));


        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = stream1.map(data -> Tuple2.of(data.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));

        stream2.keyBy(data->data.f0).window(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return Tuple2.of(stringLongTuple2.f0,stringLongTuple2.f1+t1.f1);
            }
        }).print("Window: ");
        env.execute();
    }
}
