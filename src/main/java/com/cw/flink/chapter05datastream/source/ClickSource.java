package com.cw.flink.chapter05datastream.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 注意的是 SourceFunction 接口定义的数据源，并行度只能设置为 1，如果数据源设置为大于 1 的并行度，则会抛出异常。
 * 如果我们想要自定义并行的数据源的话，需要使用 ParallelSourceFunction
 */
public class ClickSource implements SourceFunction<Event> {
    // 声明一个标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        // 随机生成数据
        Random random = new Random();
        // 定义字段选取的数据集
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        // 循环生成数据
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user, url, timestamp));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
