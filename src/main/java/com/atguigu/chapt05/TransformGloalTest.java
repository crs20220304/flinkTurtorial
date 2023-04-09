package com.atguigu.chapt05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformGloalTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        stream.global().print("Print: ").setParallelism(4);//此时并行度不起作用了。所有的数都会导入到下游的一个第一个并行度中处理。
//        Print: :1> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
//        Print: :1> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
//        Print: :1> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
//        Print: :1> Event{user='Alice', url='./prod?id=200', timestamp=1970-01-01 08:00:03.5}
//        Print: :1> Event{user='Bob', url='./prod?id=2', timestamp=1970-01-01 08:00:02.5}
//        Print: :1> Event{user='Alice', url='./prod?id=300', timestamp=1970-01-01 08:00:03.6}
//        Print: :1> Event{user='Bob', url='./home', timestamp=1970-01-01 08:00:03.0}
//        Print: :1> Event{user='Bob', url='./prod?id=1', timestamp=1970-01-01 08:00:02.3}
//        Print: :1> Event{user='Bob', url='./prod?id=3', timestamp=1970-01-01 08:00:03.3}
        env.execute();




    }
}
