package com.cw.flink.chapter07processfunction;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @description: KeyedProcessFunction-事件时间定时器（registerEventTimeTimer）
 * @author: chenwei
 * @date: 2022/9/9 14:54
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 事件时间定时器
        // KeyedProcessFunction只有在keyBy后才能使用
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    /**
                     *
                     * @param value 当前流中的输入元素，也就是正在处理的数据，类型与流中数据类型一致。
                     * @param ctx ProcessFunction 中定义的内部抽象类 {@link ProcessFunction.Context} 表示当前运行的上下文，可以获取到当前element的时间戳，
                     *            并提供了用于查询时间和注册定时器的“定时服务”{@link TimerService}
                     *            以及可以将数据发送到“侧输出流”（side output）的方法.output()
                     *            context只在调用此方法期间有效，不要存储它。
                     * @param out “收集器”（类型为 Collector），用于返回输出数据。使用方式与 flatMap算子中的收集器完全一样，直接调用 out.collect()方法就可以向下游发出一个数据。
                     *            这个方法可以多次调用，也可以不调用。通过几个参数的分析不难发现，ProcessFunction可以轻松实现flatMap这样的基本转换功能（当然 map、filter 更不在话下）；
                     *            而通过富函数提供的获取上下文方法.getRuntimeContext()，也可以自定义状态（state）进行处理，这也就能实现聚合操作的功能了。
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Long curTimestamp = ctx.timestamp();// 当前处理时间
                        out.collect(ctx.getCurrentKey() +
                                " 的数据已到达，时间戳：" + new Timestamp(curTimestamp) +
                                " watermark：" + new Timestamp(ctx.timerService().currentWatermark()));


                        // 注册一个10秒后的事件时间定时器
                        ctx.timerService().registerEventTimeTimer(curTimestamp + 10 * 1000L);

                    }


                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() +
                                " 的定时器触发，触发时间：" + new Timestamp(timestamp) +
                                " watermark:" + new Timestamp(ctx.timerService().currentWatermark()));

                    }


                }).print();


        env.execute();
    }

    // 自定义测试数据源
    public static class CustomSource implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));

            Thread.sleep(5000L);

            ctx.collect(new Event("Alice", "./home", 11000L));

            Thread.sleep(5000L);

            ctx.collect(new Event("Bob", "./home", 11001L));

            Thread.sleep(5000L);

        }

        @Override
        public void cancel() {

        }
    }
}
