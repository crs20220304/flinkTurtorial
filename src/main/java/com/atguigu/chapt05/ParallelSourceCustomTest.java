package com.atguigu.chapt05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class ParallelSourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new ParallelSourceCuctomFunction()).setParallelism(2).print("Customer");
        env.execute("Parallelcustom");
    }

    public static class  ParallelSourceCuctomFunction implements ParallelSourceFunction<Integer>{
        boolean flag=true;
        Random random= new Random();
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while (flag) {
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
              flag=false;
        }
    }

}
