package com.cw.flink.chapter06watermark.watermark;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @description: --------------------------------------
 * TODO 1.三种时间策略
 * 事件时间（Event Time）：数据产生的时间，一个数据产生的时刻，就是流处理中事件触发的时间点，这就是“事件时间”，一般都会以时间戳的形式作为一个字段记录在数据里。
 * 处理时间（Processing Time）：数据真正被处理的时间
 * 摄入时间（Ingestion Time）：数据进入 Flink 数据流的时间，也就是 Source 算子读入数据的时间。
 * 在 Flink 中，由于处理时间比较简单，早期版本默认的时间语义是处理时间；而考虑到事件时间在实际应用中更为广泛，
 * 从 1.12 版本开始，Flink 已经将事件时间作为了默认的时间语义。
 * <p>
 * TODO 2.水位线生成策略（Watermark Strategies）
 * 在Flink的DataStream API中，有一个单独用于生成水位线的方法：.assignTimestampsAndWatermarks()，
 * 它主要用来为流中的数据分配时间戳，并生成水位线来指示事件时间。
 * .assignTimestampsAndWatermarks()方法需要传入一个WatermarkStrategy作为参数，这就是所谓的“水位线生成策略”。
 * @author:chenwei
 * @date:2022/9/6 14:27
 */
public class WatermarkTest_01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         onPeriodicEmit：周期性调用的方法，可以由 WatermarkOutput 发出水位线。
         周期时间为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置。
         生成水位线的时间间隔，默认为200ms。
         */
        env.getConfig().setAutoWatermarkInterval(100);

        // 从元素中读取操作
        /**
         TODO 1.有序流的watermark生成
         */
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
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        /**
                         对于有序流，主要特点就是时间戳单调增长（Monotonously Increasing Timestamps），所以永远不会出现迟到数据的问题。
                         这是周期性生成水位线的最简单的场景，直接调用WatermarkStrategy.forMonotonousTimestamps()方法就可以实现。
                         简单来说，就是直接拿当前最大的时间戳作为水位线就可以了。
                         */
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long recordTimestamp) {
                                        return event.timestamp;
                                    }
                                })
                )
                .print("monotonousTimestamps");


        /**
         TODO 2.乱序流的watermark生成
         */
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
                .assignTimestampsAndWatermarks(
                        /**
                         由于乱序流中需要等待迟到数据到齐，所以必须设置一个固定量的延迟时间（ Fixed Amount of Lateness）。
                         这时生成水位线的时间戳，就是当前数据流中最大的时间戳减去延迟的结果，相当于把表调慢，当前时钟会滞后于数据的最大时间戳。
                         调用WatermarkStrategy.forBoundedOutOfOrderness()方法就可以实现。
                         这个方法需要传入一个 maxOutOfOrderness 参数，表示“最大乱序程度”，它表示数据流中乱序数据时间戳的最大差值；
                         如果我们能确定乱序程度，那么设置对应时间长度的延迟，就可以等到所有的乱序数据了。

                         有序流的水位线生成器本质上和乱序流是一样的，相当于延迟设为 0 的乱序流水位线生成器，两者完全等同
                         WatermarkStrategy.forMonotonousTimestamps()
                         WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))

                         乱序流中生成的水位线真正的时间戳 = 当前最大时间戳 – 延迟时间 – 1
                         public void onPeriodicEmit(WatermarkOutput output) {
                         output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
                         }
                         */
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(
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
                .print("boundedOutOfOrderness");

        env.execute();


    }
}
