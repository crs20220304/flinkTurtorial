package com.atguigu.chapter06.reduceaggregate;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class AggregationFunction {
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


        SingleOutputStreamOperator<String> stream2 = stream1.keyBy(data -> data.user).window(TumblingEventTimeWindows.of(Time.seconds(1))).aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {
            //生成对应窗口的累加器，每个窗口对应一个。只会调用一次。
            String key="";
            @Override
            public Tuple2<Long, Integer> createAccumulator() {
                return Tuple2.of(0L, 0);
            }

            @Override
            public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> longIntegerTuple2) {
                key=event.user;
                return Tuple2.of(longIntegerTuple2.f0 + event.timestamp, longIntegerTuple2.f1 + 1);
            }

            // 对应窗口的 输出方法，每个窗口对应一个。只会窗口关闭触发时调用一次。
            @Override
            public String getResult(Tuple2<Long, Integer> longIntegerTuple2) {
                return key+" "+ new Timestamp(longIntegerTuple2.f0 / longIntegerTuple2.f1).toString();
            }

            //窗口合并的时候触发。
            @Override
            public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> longIntegerTuple2, Tuple2<Long, Integer> acc1) {
                return Tuple2.of(longIntegerTuple2.f0 + acc1.f0, longIntegerTuple2.f1 + acc1.f1);
            }
        });
        stream2.print("aggregation: ");
        env.execute();
    }
}
