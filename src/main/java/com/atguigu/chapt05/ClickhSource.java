package com.atguigu.chapt05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickhSource implements SourceFunction<Event>{
    Boolean flag=true;
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        while (flag) {
            sourceContext.collect(
                    new Event(users[new Random().nextInt(users.length)],
                    urls[new Random().nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
       flag=false;
    }
}
