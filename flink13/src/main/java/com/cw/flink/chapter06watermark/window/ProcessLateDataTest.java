package com.cw.flink.chapter06watermark.window;

import com.cw.flink.chapter05datastream.source.Event;
import com.cw.flink.chapter06watermark.window.entity.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @description:
 * @author:chenwei
 * @date:2022/9/8 10:57
 */
public class ProcessLateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 生成水位线的时间间隔，默认为200ms。
        env.getConfig().setAutoWatermarkInterval(100);


//        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("10.13.33.126", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                    }
                })
                // 方式一：设置watermark延迟时间，2秒钟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                                        // 针对乱序流插入水位线，延迟时间设置为 2s，水位线真正的时间戳=当前最大时间戳 – 延迟时间 – 1
                                        Duration.ofSeconds(2)
                                )
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event event, long recordTimestamp) {
                                        return event.timestamp;
                                    }
                                })
                );

        stream.print("input");

        // 定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("late") {
        };

        // 统计每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                // 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 方式二：允许窗口处理迟到数据，设置1分钟的等待时间
                .allowedLateness(Time.minutes(1))
                // 方式三：将最后的迟到数据输出到侧输出流
                .sideOutputLateData(late)
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());

        result.print("result");

        /*
            事件时间为72s时，第一个窗口才关闭，之后出现的小于72s的数据就会放到侧输出流(sideOutputLateData)中
            72s =
            60s(allowedLateness)
            + 10s(TumblingEventTimeWindows.of(Time.seconds(10)))
            + 2s(Duration.ofSeconds(2))
         */
        result.getSideOutput(late).print("late");

        env.execute();
    }
}
