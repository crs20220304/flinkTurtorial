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

public class URLViewCountTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(100);
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickhSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));
        stream.keyBy(k->k.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new URLAggregationFunction(),new UrlViewCountResult()).print("new : ");
        env.execute();

    }

    public static class URLAggregationFunction implements AggregateFunction<Event,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long aLong) {
            return  aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }


  public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

    @Override
    public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
        // 结合窗口信息，包装输出内容
        Long start = context.window().getStart();
        Long end = context.window().getEnd();
        // 迭代器中只有一个元素，就是增量聚合函数的计算结果
        out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
    }
}
}
