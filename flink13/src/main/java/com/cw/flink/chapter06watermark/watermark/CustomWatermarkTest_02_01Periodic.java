package com.cw.flink.chapter06watermark.watermark;

import com.cw.flink.chapter05datastream.source.ClickSource;
import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 自定义水位线的产生（周期性生成水位线）
 * 在 WatermarkStrategy 中，时间戳分配器 TimestampAssigner 都是大同小异的，指定字段提取时间戳就可以了；
 * 而不同策略的关键就在于 WatermarkGenerator 的实现。
 * 整体说来，Flink有两种不同的生成水位线的方式：一种是周期性的（Periodic），另一种是断点式的（Punctuated）。
 * <p>
 * WatermarkGenerator 接口中有两个方法onEvent()和 onPeriodicEmit()，
 * 前者是在每个事件到来时调用，而后者由框架周期性调用。周期性调用的方法中发出水位线，自然就是周期性生成水位线；
 * 而在事件触发的方法中发出水位线，自然就是断点式生成了。两种方式的不同就集中体现在这两个方法的实现上。
 *
 * @author:chenwei
 * @date:2022/9/8 15:12
 */
public class CustomWatermarkTest_02_01Periodic {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                /**
                 * assignTimestampsAndWatermarks()方法需要传入一个 WatermarkStrategy 作为参数，这就是所谓的 “水位线生成策略 ” 。
                 */
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy()).print();

        env.execute();
    }

    /**
     * public interface WatermarkStrategy<T> extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T>
     * <p>
     * WatermarkStrategy中包含了一个“时间戳分配器”TimestampAssigner和一个“水位线生成器”WatermarkGenerator
     */
    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        /**
         * TimestampAssigner：主要负责从流中数据元素的某个字段中提取时间戳，并分配给元素。时间戳的分配是生成水位线的基础。
         */
        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp; // 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }

        /**
         * WatermarkGenerator： 主要负责按照既定的方式， 基于时间戳生成水位线。
         * 在WatermarkGenerator 接口中，主要又有两个方法：onEvent()和 onPeriodicEmit()。
         */
        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }
    }

    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

        /**
         * onEvent：每个事件（数据）到来都会调用的方法，它的参数有当前事件、时间戳， 以及允许发出水位线的一个 WatermarkOutput，可以基于事件做各种操作
         *
         * @param event
         * @param eventTimestamp
         * @param output
         */
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.timestamp, maxTs); // 更新最大时间戳
        }

        /**
         * onPeriodicEmit：周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间为处理时间，
         * 可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为200ms。
         * env.getConfig().setAutoWatermarkInterval(60 * 1000L);
         *
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线，默认200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}
