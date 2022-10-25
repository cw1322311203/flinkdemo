package com.cw.flink.chapter05datastream.transformation;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @description: flatMap 并没有直接定义返回值类型，而是通过一个“收集器”（Collector）来指定输出。
 * 希望输出结果时，只要调用收集器的.collect()方法就可以了；
 * 这个方法可以多次调用，也可以不调用。所以 flatMap 方法也可以实现 map 方法和 filter 方法的功能，
 * 当返回结果是 0 个的时候，就相当于对数据进行了过滤，
 * 当返回结果是 1 个的时候，相当于对数据进行了简单的转换操作。
 *
 * TODO 注意：使用lambda表达式实现时，需要使用returns方法指定转换后类型
 * @author: chenwei
 * @date: 2022/9/1 15:11
 */
public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取操作
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 1.实现FlatMapFunction
        stream.flatMap(new MyFlatMap()).print("1");

        // 2.传入lambda表达式，注意使用returns指定转换后类型
        stream.flatMap((Event event, Collector<String> collector) -> {
                    if (event.user.equals("Mary")) {
                        collector.collect(event.url);
                    } else if (event.user.equals("Bob")) {
                        collector.collect(event.user);
                        collector.collect(event.url);
                        collector.collect(event.timestamp.toString());
                    }
                })
                /**
                 * Flink 专门提供了TypeHint 类，它可以捕获泛型的类型信息，并且一直记录下来，为运行时提供足够的信息。
                 * 我们同样可以通过.returns()方法，明确地指定转换之后的 DataStream 里元素的类型。
                 */
                .returns(new TypeHint<String>() {
                })
                .print("2");

        // 3.直接实现FlatMapFunction
        stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                if (event.user.equals("Mary")) {
                    collector.collect(event.url);
                } else if (event.user.equals("Bob")) {
                    collector.collect(event.user);
                    collector.collect(event.url);
                    collector.collect(event.timestamp.toString());
                }
            }
        }).print("3");

        env.execute();

    }

    private static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}
