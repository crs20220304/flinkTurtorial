package com.atguigu.chapter06.reduceaggregate;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowAggregationFunctionTestPU_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //需要设置时间语义


        SingleOutputStreamOperator stream = env.addSource(new ClickhSource()).returns(TypeInformation.of(new TypeHint<Event>() {}));
        SingleOutputStreamOperator<Event> stream1 = stream.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Event>forBoundedOutOfOrderness(Duration.ZERO).
                withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                          @Override
                                          public long extractTimestamp(Event event, long l) {
                                              return event.timestamp;
                                          }
                                      }
                ));

        stream1.print("stream1: ");
        SingleOutputStreamOperator<Double> stream2 = stream1.keyBy(data -> "1")
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new MyAggregation());
        stream2.print("aggregation: ");
        env.execute();
    }

    public static class MyAggregation implements  AggregateFunction<Event,Tuple2<Long, HashSet<String>>,Double>{


        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L,new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
                longHashSetTuple2.f1.add(event.user);
            return  Tuple2.of(longHashSetTuple2.f0+1,longHashSetTuple2.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            return Double.valueOf(longHashSetTuple2.f0/longHashSetTuple2.f1.size());
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }

}
