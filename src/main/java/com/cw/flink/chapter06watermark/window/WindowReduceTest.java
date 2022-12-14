package com.cw.flink.chapter06watermark.window;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @description:
 * @author:chenwei
 * @date:2022/9/6 14:27
 */
public class WindowReduceTest {
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

        stream
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 滚动事件时间窗口
//                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))// 滑动事件时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2))) // 事件时间会话窗口
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 滚动处理时间窗口
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) // 滑动处理时间窗口
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))) // 处理时间会话窗口
//                .window(GlobalWindows.create());// 全局窗口，必须自行定义触发器才能实现窗口计算
//                .countWindow(10) // 计数窗口
//                .countWindow(10,2) // 滑动计数窗口
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print()
        ;

        env.execute();


    }
}
