package com.atguigu.chapt05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

public class TransFromRichTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );
        clicks.map(new MyRichFunction()).setParallelism(2).print("MyRich: ");
        env.execute();

    }

    //每个并行度任务调用一次open和close方法。只调用一次。开始和结束时分别调用。

    public  static class MyRichFunction extends RichMapFunction<Event, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("索引为： "+getRuntimeContext().getIndexOfThisSubtask()+"号被调用");
        }

        @Override
        public void close() throws Exception {
            System.out.println("索引为： "+getRuntimeContext().getIndexOfThisSubtask()+"号被关闭");
        }

        @Override
        public Integer map(Event event) throws Exception {
              return  event.url.length();
        }
    }

}
