package com.atguigu.chapter06.windowall;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class ProcessWindowFunctionTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickhSource()).
                assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

         stream.keyBy(data -> true).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new MyProcessWindowFunction()).print("process: ");
         env.execute();

    }
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Event,String,Boolean, TimeWindow>{
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            HashSet<String> h = new HashSet<>();
            for (Event event : iterable) {
                h.add(event.user);
            }
            //结合窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口： "+new Timestamp(start)+"~"+new Timestamp(end)+"UV值为： "+h.size());
        }
    }



}
