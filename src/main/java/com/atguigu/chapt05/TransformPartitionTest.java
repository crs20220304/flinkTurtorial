package com.atguigu.chapt05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> st1 = env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 1; i < 9; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        sourceContext.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2);
//        st1.rebalance().print("Reblance: ").setParallelism(4);
//        st1.print("Original: ").setParallelism(2);//print()算子后面的并行度决定最终的并行度。默认是cpu核数。
        st1.rescale().print("Rescale: ").setParallelism(4);//2,4.6.8给1，2并行度。


        env.execute();


    }
}
