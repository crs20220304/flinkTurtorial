package com.atguigu.chapt05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new ClickhSource()).print("Customer");// The parallelism of non parallel operator must be 1.
//        DataStreamSource stream1 = env.addSource(new ClickhSource());
//        stream1.print();
//        env.execute();
    }
}
