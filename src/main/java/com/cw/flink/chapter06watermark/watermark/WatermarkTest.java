package com.cw.flink.chapter06watermark.watermark;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @description:
 * @author:chenwei
 * @date:2022/9/6 14:27
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 生成水位线的时间间隔，默认为200ms。
        env.getConfig().setAutoWatermarkInterval(100);

        // 从元素中读取操作
        env.fromElements(
                        new Event("Mary", "./home", 1000L),
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./prod?id=100", 3000L),
                        new Event("Bob", "./prod?id=1", 3300L),
                        new Event("Alice", "./prod?id=200", 3200L),
                        new Event("Bob", "./home", 3500L),
                        new Event("Bob", "./prod?id=2", 3800L),
                        new Event("Bob", "./prod?id=3", 4200L)
                )
//                // 有序流的watermark生成
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event event, long recordTimestamp) {
//                                return event.timestamp;
//                            }
//                        })
//                )
                /*
                    乱序流的watermark生成，水位线真正的时间戳=当前最大时间戳 – 延迟时间 – 1
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
                    }
                 */
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
                )
                .print();

        env.execute();


    }
}
