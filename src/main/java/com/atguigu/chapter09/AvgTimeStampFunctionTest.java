package com.atguigu.chapter09;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AvgTimeStampFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickhSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
        stream.print("input: ");
        stream.keyBy(data->data.user).flatMap(new MyFlatMapFunctionTest(5L)).print();


        env.execute();

    }

    //最近连续五个数据的时间戳的平均值.
   public static class  MyFlatMapFunctionTest extends RichFlatMapFunction<Event,String>{

        private Long count;
        ValueState<Long> valueState;
        AggregatingState<Event, Long> aggregationState;

        public MyFlatMapFunctionTest(Long count) {
           this.count = count;
       }





        @Override
        public void open(Configuration parameters) throws Exception {
             valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState", Long.class));
             aggregationState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>("aggregationState", new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                @Override
                public Tuple2<Long, Long> createAccumulator() {
                    return Tuple2.of(0L,0L);
                }

                @Override
                public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                    return Tuple2.of(accumulator.f0+value.timestamp,accumulator.f1+1);
                }

                @Override
                public Long getResult(Tuple2<Long, Long> accumulator) {
                    return  accumulator.f0/accumulator.f1;
                }

                @Override
                public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                    return null;
                }
            }, Types.TUPLE(Types.LONG, Types.LONG)));

        }

        @Override
       public void flatMap(Event value, Collector<String> out) throws Exception {
            Long value1 = valueState.value();
            if(value1==null){
                value1=1L;
            }else
                value1++;
            //更新状态.
            valueState.update(value1);
            aggregationState.add(value);

            if(count.equals(valueState.value())){
                out.collect(value.user+"过去："+count+"次访问平均时间戳： "+aggregationState.get());
                valueState.clear();
                aggregationState.clear();
            }

        }
   }

}
