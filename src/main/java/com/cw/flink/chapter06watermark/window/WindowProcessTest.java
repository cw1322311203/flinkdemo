package com.cw.flink.chapter06watermark.window;

import com.cw.flink.chapter05datastream.source.ClickSource;
import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @description:
 * @author:chenwei
 * @date:2022/9/7 15:55
 */
public class WindowProcessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 生成水位线的时间间隔，默认为200ms。
        env.getConfig().setAutoWatermarkInterval(100);


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                                        // 有序流处理
                                        Duration.ZERO
                                )
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event event, long recordTimestamp) {
                                        return event.timestamp;
                                    }
                                })
                );

        stream.print("data");

        // 使用ProcessWindowFunction计算UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                .print();

        env.execute();
    }

    // 实现自定义ProcessWindowFunction，自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // 使用HashSet保存user
            HashSet<String> userSet = new HashSet<>();
            // 从elements中遍历数据，放到set中去重
            for (Event event : elements) {
                userSet.add(event.user);
            }

            Integer uvCount = userSet.size();

            // 结合窗口信息
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect("窗口【" + new Timestamp(start) + " ~ " + new Timestamp(end) + "】的UV值为:" + uvCount);


        }
    }


}
