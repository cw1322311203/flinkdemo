package com.cw.flink.chapter07processfunction;

import com.cw.flink.chapter05datastream.source.ClickSource;
import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @description: KeyedProcessFunction-处理时间定时器（registerProcessingTimeTimer）
 * <p>
 * 在 Flink 程序中，为了实现数据的聚合统计，或者开窗计算之类的功能，我们一般都要先用 keyBy 算子对数据流进行“按键分区”，
 * 得到一个 KeyedStream。也就是指定一个键（key），按照它的哈希值（hash code）将数据分成不同的“组”，然后分配到不同的并行子任务上执行计算；
 * 这相当于做了一个逻辑分流的操作，从而可以充分利用并行计算的优势实时处理海量数据。
 * 另外我们在上节中也提到，只有在 KeyedStream 中才支持使用TimerService 设置定时器的操作。所以一般情况下，我们都是先做了 keyBy 分区之后，再去定义处理操作；
 * 代码中更加常见的处理函数是KeyedProcessFunction，最基本的 ProcessFunction 反而出镜率没那么高。
 * @author: chenwei
 * @date: 2022/9/9 11:15
 */
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

                        Long curTimestamp = ctx.timerService().currentProcessingTime();// 当前处理时间
                        out.collect(ctx.getCurrentKey() + " 的数据已到达，到达时间：" + new Timestamp(curTimestamp));

                        // 注册一个10秒后的处理时间定时器
                        ctx.timerService().registerProcessingTimeTimer(curTimestamp + 10 * 1000L);
                        // 注册一个10秒后的事件时间定时器
                        //ctx.timerService().registerEventTimeTimer(curTimestamp + 10 * 1000L);
                        //删除触发时间为 time 的处理时间定时器
                        //ctx.timerService().deleteEventTimeTimer(long time);
                        // 删除触发时间为 time 的事件时间定时器
                        //ctx.timerService().deleteEventTimeTimer(long time);

                        /**
                         * 尽管处理函数中都可以直接访问TimerService，不过只有基于 KeyedStream 的处理函数，
                         * 才能去调用注册和删除定时器的方法； 未作按键分区的DataStream 不支持定时器操作，只能获取当前时间。
                         */
                        // 获取当前的处理时间
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        // 获取当前的水位线（事件时间）
                        long currentWatermark = ctx.timerService().currentWatermark();
                    }

                    /**
                     * KeyedProcessFunction 的一个特色，就是可以灵活地使用定时器。
                     * 定时器触发时的操作,只有“按键分区流”KeyedStream才支持设置定时器的操作
                     * @param timestamp 触发计时器的时间戳。设定好的触发时间，事件时间语义下当然就是水位线了
                     * @param ctx An {@link ProcessFunction.OnTimerContext} that allows querying the timestamp of the firing timer,
                     *     querying the {@link TimeDomain} of the firing timer and getting a {@link TimerService}
                     *     for registering timers and querying the time. The context is only valid during the
                     *     invocation of this method, do not store it.
                     * @param out “收集器”（类型为 Collector），用于返回输出数据
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 的定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
