package com.atguigu.chapter06;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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

public class AggregateAndWindallTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickhSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));
        stream.print();
        stream.keyBy(k->true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //注意一个是实现，一个是继承。前面聚合的结果会传递给下一个new UVProcessWindowFunction()参数的集合里面，该集合只会有一个值。
                //该种方法，可以对数据开窗，每来一个数据就累计一次，到最后窗口关闭时，数据已经累计完毕，将结果传给后面的参数中。这个参数可以获得上下文信息。获取的信息就更全面。
                .aggregate(new UVMYAggregationFunction(),new UVProcessWindowFunction())
                .print("all: ");
        env.execute();
    }

    public static class UVMYAggregationFunction implements AggregateFunction<Event, HashSet<String>,Long>{
        @Override
        public HashSet<String> createAccumulator() {
            return  new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> strings) {
            strings.add(event.user);
            return  strings;
        }

        @Override
        public Long getResult(HashSet<String> strings) {
            return  Long.valueOf(strings.size());
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }

    public static class UVProcessWindowFunction extends ProcessWindowFunction<Long,String,Boolean, TimeWindow> {


        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            //结合窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口： "+new Timestamp(start)+"~"+new Timestamp(end)+"UV值为： "+iterable.iterator().next());
        }
    }
}
