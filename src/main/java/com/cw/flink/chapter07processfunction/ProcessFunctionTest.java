package com.cw.flink.chapter07processfunction;

import com.cw.flink.chapter05datastream.source.ClickSource;
import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @description: ProcessFunction介绍
 * @author: chenwei
 * @date: 2022/9/8 16:55
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 定义一个侧输出流的标识
        OutputTag<Event> outputTag = new OutputTag<Event>("outputTag") {
        };

        SingleOutputStreamOperator<String> processStream = stream.process(new ProcessFunction<Event, String>() {
            /**
             *
             * @param value 当前流中的输入元素，也就是正在处理的数据，类型与流中数据类型一致。
             * @param ctx ProcessFunction 中定义的内部抽象类 {@link Context} 表示当前运行的上下文，可以获取到当前element的时间戳，
             *            并提供了用于查询时间和注册定时器的“定时服务”{@link TimerService}(定时器相关操作只能在KeyedProcessFunction中使用)
             *            以及可以将数据发送到“侧输出流”（side output）的方法.output()
             *            context只在调用此方法期间有效，不要存储它。
             * @param out “收集器”（类型为 Collector），用于返回输出数据。使用方式与 flatMap算子中的收集器完全一样，直接调用 out.collect()方法就可以向下游发出一个数据。
             *            这个方法可以多次调用，也可以不调用。通过几个参数的分析不难发现，ProcessFunction可以轻松实现flatMap这样的基本转换功能（当然 map、filter 更不在话下）；
             *            而通过富函数提供的获取上下文方法.getRuntimeContext()，也可以自定义状态（state）进行处理，这也就能实现聚合操作的功能了。
             * @throws Exception
             */
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                if (value.user.equals("Mary")) {
                    out.collect("-------------------------");
                    out.collect(value.user + " clicks " + value.url);
                } else if (value.user.equals("Bob")) {
                    out.collect("-------------------------");
                    out.collect(value.user);
                    out.collect(value.user);
                } else if ("Cary".equals(value.user)) {
                    // 如果user为Cary那么就写入到侧输出流
                    out.collect("-------------------------");
                    ctx.output(outputTag, value);
                }
                out.collect(value.toString());
                // 获取当前处理时间
                System.out.println("ctx.timestamp() = " + ctx.timestamp());
                // 获取当前的处理时间
                System.out.println("ctx.timerService().currentProcessingTime() = " + ctx.timerService().currentProcessingTime());
                // 获取当前的水位线（事件时间）
                System.out.println("ctx.timerService().currentWatermark() = " + ctx.timerService().currentWatermark());

                System.out.println("getRuntimeContext().getIndexOfThisSubtask() = " + getRuntimeContext().getIndexOfThisSubtask());


            }

            /**
             *
             * @param parameters The configuration containing the parameters attached to the contract.
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        processStream.print();

        // 得到侧输出流的数据
        DataStream<Event> sideOutput = processStream.getSideOutput(outputTag);
        sideOutput.print("outputTag");

        env.execute();
    }
}
