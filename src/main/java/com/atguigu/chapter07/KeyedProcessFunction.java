package com.atguigu.chapter07;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<Event> stream = env.addSource(new ClickhSource());
        stream.
                keyBy(data->data.user).
                 process(new org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Event, String>() {


                     @Override
            public void processElement(Event event, org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(context.getCurrentKey() + " 数据到达，到达时间： " + new Timestamp(context.timerService().currentProcessingTime()));
                //注册一个十秒后触发的定时器,注意区分事件事件与处理时间定时器
                         context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+10*1000L);
            }
                     @Override
                     public void onTimer(long timestamp, org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

                         out.collect(ctx.getCurrentKey()+"定时器触发，出发时间： "+new Timestamp(timestamp));
                     }
        }).print("keyedProcessFunction: ");
        env.execute();

    }
}
