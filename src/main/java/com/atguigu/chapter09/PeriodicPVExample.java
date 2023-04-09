package com.atguigu.chapter09;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PeriodicPVExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickhSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
        stream.print("input: ");
        stream.
                keyBy(data->data.user).
                process(new MyCustomerKeyedProcessFunction()).print("HelloWorld:");
        env.execute();
    }
    public static class MyCustomerKeyedProcessFunction extends KeyedProcessFunction<String, Event, String> {
        ValueState<Long> countValueState;
        ValueState<Long> timestampValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("myValue:", Long.class));
            timestampValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("myTimeStampValue:", Long.class));
        }

        @Override
        public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
            Long value = countValueState.value();
            countValueState.update(value==null? 1 : value + 1);

            if(timestampValueState==null){
                context.timerService().registerEventTimeTimer(event.timestamp+10*1000L);
                timestampValueState.update(event.timestamp+10*1000L);
            }

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
             out.collect("当前key："+ctx.getCurrentKey()+"从开始至今的计数值是："+countValueState.value());
            timestampValueState.clear();
            ctx.timerService().registerProcessingTimeTimer(timestamp+10*1000L);
            timestampValueState.update(timestamp+10*1000L);

        }
    }
}
