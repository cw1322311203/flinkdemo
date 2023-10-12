package com.cw.flink.chapter06watermark.watermark;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;


import java.util.Calendar;
import java.util.Random;

/**
 * TODO 在自定义数据源中发送水位线
 * 在自定义水位线中生成水位线相比 assignTimestampsAndWatermarks 方法更加灵活，可以任意的产生周期性的、非周期性的水位线，
 * 以及水位线的大小也完全由我们自定义。所以非常适合用来编写 Flink 的测试程序，测试 Flink 的各种各样的特性。
 *
 * @author:chenwei
 * @date:2022/9/8 15:14
 */
public class EmitWatermarkInSourceFunction_03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSourceWithWatermark()).print();

        env.execute();
    }

    // 泛型是数据源中的类型
    public static class ClickSourceWithWatermark implements SourceFunction<Event> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                long currTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间戳
                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, currTs);

                /**
                 使用collectWithTimestamp方法将数据发送出去，并指明数据中的时间戳的字段
                 sourceContext.collect(Event event)：只发送数据
                 */
                sourceContext.collectWithTimestamp(event, event.timestamp);

                // 发送水位线
                sourceContext.emitWatermark(new Watermark(event.timestamp - 1L));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
