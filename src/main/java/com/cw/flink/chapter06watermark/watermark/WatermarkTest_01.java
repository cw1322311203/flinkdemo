package com.cw.flink.chapter06watermark.watermark;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @description: --------------------------------------
 * 事件时间（Event Time）：数据产生的时间
 * > 一个数据产生的时刻，就是流处理中事件触发的时间点，这就是“事件时间”，一般都会以时间戳的形式作为一个字段记录在数据里。
 * 处理时间（Processing Time）：数据真正被处理的时间
 * 摄入时间（Ingestion Time）：数据进入 Flink 数据流的时间，也就是 Source 算子读入数据的时间。
 * 在 Flink 中，由于处理时间比较简单，早期版本默认的时间语义是处理时间；而考虑到事件时间在实际应用中更为广泛，
 * 从 1.12 版本开始，Flink 已经将事件时间作为了默认的时间语义。
 * @author:chenwei
 * @date:2022/9/6 14:27
 */
public class WatermarkTest_01 {
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
                /**
                 * 在Flink的DataStream API中，有一个单独用于生成水位线的方法：.assignTimestampsAndWatermarks()，
                 * 它主要用来为流中的数据分配时间戳，并生成水位线来指示事件时间。
                 * .assignTimestampsAndWatermarks()方法需要传入一个WatermarkStrategy作为参数，这就是所谓的“水位线生成策略”。
                 */
                /*
                 有序流的watermark生成
                */
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.timestamp;
                            }
                        })
                )
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
