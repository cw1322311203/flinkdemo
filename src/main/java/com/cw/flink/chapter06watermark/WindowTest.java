package com.cw.flink.chapter06watermark;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @description:
 * @author:chenwei
 * @date:2022/9/6 14:27
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 生成水位线的时间间隔，默认为200ms。
        env.getConfig().setAutoWatermarkInterval(100);

        // 从元素中读取操作
        SingleOutputStreamOperator<Event> stream = env.fromElements(
                        new Event("Mary", "./home", 1000L),
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./prod?id=100", 3000L),
                        new Event("Bob", "./prod?id=1", 3300L),
                        new Event("Alice", "./prod?id=200", 3200L),
                        new Event("Bob", "./home", 3500L),
                        new Event("Bob", "./prod?id=2", 3800L),
                        new Event("Bob", "./prod?id=3", 4200L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                                        // 针对乱序流插入水位线，延迟时间设置为 5s
                                        Duration.ofSeconds(5)
                                )
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event event, long recordTimestamp) {
                                        return event.timestamp;
                                    }
                                })
                );

        stream.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 滚动事件时间窗口
//                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))// 滑动事件时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2))) // 事件时间会话窗口
//                .countWindow(10,2) // 滑动计数窗口
        ;

        env.execute();


    }
}
