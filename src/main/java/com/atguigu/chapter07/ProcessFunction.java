package com.atguigu.chapter07;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickhSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));
        stream.process(new org.apache.flink.streaming.api.functions.ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, org.apache.flink.streaming.api.functions.ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                if(event.user.equals("Mary")){
                    collector.collect(event.user+" clicks "+event.url);
                }else if(event.user.equals("Bob")){
                   collector.collect(event.user);
                }
                collector.collect(event.toString());
                System.out.println("timestamp: " + context.timestamp());
                System.out.println("waterMark: " + context.timerService().currentWatermark());

            }
        }).print();
        env.execute();

    }
}
