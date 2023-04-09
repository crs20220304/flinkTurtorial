package com.atguigu.chapter09;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickhSource()).
                assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).
                        withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.keyBy(data->data.user).flatMap(new MyflatMap()).print("CunstomerState: ");
        env.execute();

    }
    public static class MyflatMap extends RichFlatMapFunction<Event, String> {
        //定义状态.
        ValueState<Event> stateCustomer;
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            System.out.println("之前的： "+stateCustomer.value());
            stateCustomer.update(event);
            System.out.println("更新后的状态: " + stateCustomer.value());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            stateCustomer = getRuntimeContext().getState(new ValueStateDescriptor("myflat: ",Event.class));

        }
    }
}
