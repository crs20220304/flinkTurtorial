package com.atguigu.wc;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.setRuntimeMode(RuntimeExecutionMode.BATCH); 设定为batch模式
       // DataStreamSource<String> ds1 = env.socketTextStream("hadoop102", 9000);
        DataStreamSource<String> ds1 = env.readTextFile("input/wc.txt");
        ds1.flatMap((String line, Collector<Tuple2<String,Long>>out)->{
            String[] s = line.split(" ");
            for (String s1 : s) {
                out.collect(Tuple2.of(s1,1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG)).keyBy( x->x.f0).sum(1).print();
        env.execute();
    }
}
