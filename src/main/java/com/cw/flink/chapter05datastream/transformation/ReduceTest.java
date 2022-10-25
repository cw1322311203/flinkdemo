package com.cw.flink.chapter05datastream.transformation;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Type;

/**
 * @description: reduce：对已有的数据进行归约处理，把每一个新输入的数据和当前已经归约出来的值，再做一个聚合计算。
 * 与简单聚合类似，reduce 操作也会将 KeyedStream 转换为 DataStream。它不会改变流的元素数据类型，所以输出类型和输入类型是一样的。
 * ReduceFunction 接口里需要实现 reduce()方法，这个方法接收两个输入事件，经过转换处理之后输出一个相同类型的事件；
 * 所以，对于一组数据，我们可以先取两个进行合并，然后再将合并的结果看作一个数据、再跟后面的数据合并，最终会将它“简化”成唯一的一个数据， 这也就是 reduce“归约”的含义。
 * 在流处理的底层实现过程中，实际上是将中间“合并的结果” 作为任务的一个状态保存起来的；之后每来一个新的数据，就和之前的聚合状态进一步做归约。
 * @author:chenwei
 * @date:2022/9/1 16:19
 */
public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取操作
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        /**
         *public interface ReduceFunction<T> extends Function, Serializable {
         *     T reduce(T value1, T value2) throws Exception;
         * }
         *
         * 与简单聚合类似，reduce 操作也会将 KeyedStream 转换为 DataStrema。它不会改变流的元素数据类型，所以输出类型和输入类型是一样的
         */

        // 1.统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        // 每到一条数据，用户 pv 的统计值加 1
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                });
        clicksByUser.print("clicksByUser: ");

        // 2.选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser
                // 为每一条数据分配同一个 key，将聚合结果发送到一条流中
                .keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        // 将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                        return t1.f1 > t2.f1 ? t1 : t2;
                    }
                });

        result.print("result: ");

        env.execute();

    }
}
