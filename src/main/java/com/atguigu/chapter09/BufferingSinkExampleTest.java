package com.atguigu.chapter09;

import com.atguigu.chapt05.ClickhSource;
import com.atguigu.chapt05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExampleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickhSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
        stream.addSink(new MyBufferingSink(10));


        env.execute();

    }
    public static class MyBufferingSink implements SinkFunction<Event>, CheckpointedFunction{
        //定义当前类的属性，批量
        private final int threshold;
        private List<Event> bufferedElements;
        ListState<Event> listState;


        public MyBufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<Event>();
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
             listState = functionInitializationContext.getOperatorStateStore().getListState(new ListStateDescriptor<Event>("myListStat:", Event.class));
            //functionInitializationContext.isRestored()是否恢复备份的状态。
             if(functionInitializationContext.isRestored()){
                 for (Event event : listState.get()) {
                       bufferedElements.add(event);
                 }
             }

        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            listState.clear();
            for (Event bufferedElement : bufferedElements) {
                listState.add(bufferedElement);
            }

        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
              bufferedElements.add(value);
              if(bufferedElements.size()==threshold){
                  for (Event bufferedElement : bufferedElements) {
                      System.out.println(bufferedElement);
                  }
                  System.out.println("===========数据输出完毕============");
                  bufferedElements.clear();
              }


        }
    }
}
