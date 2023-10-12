package com.cw.flink.chapter05datastream.transformation;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @description: Flink 还具有一个类型提取系统，可以分析函数的输入和返回类型，自动获取类型信息，
 * 从而获得对应的序列化器和反序列化器。但是，由于 Java 中泛型擦除的存在，
 * 在某些特殊情况下（比如 Lambda 表达式中），自动提取的信息是不够精细的——只告诉 Flink 当前的元素由“船头、船身、船尾”构成，根本无法重建出“大船”的模样；
 * 这时就需要显式地提供类型信息，才能使应用程序正常工作或提高其性能。
 * 方式1
 * .map(word -> Tuple2.of(word, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));
 * 方式2
 * returns(new TypeHint<Tuple2<Integer, SomeType>>(){})
 * @author: chenwei
 * @date: 2022/10/26 10:16
 */
public class TypeHintTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取操作
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        /**
         * map 函数使用 Lambda 表达式，返回简单类型，不需要进行类型声明
         */
        DataStream<String> stream1 = stream.map(event -> event.url);
        stream1.print("stream1");

        /**
         * 类似声明方式 1 Types
         *
         * 当使用 map() 函数返回 Flink 自定义的元组类型时需要进行类型声明
         * 下例中的函数签名 Tuple2<String, Long> map(Event value) 被类型擦除为 Tuple2 map(Event value)。
         *
         */
        // 使用 map 函数也会出现类似问题，以下代码会报错
        DataStream<Tuple2<String, Long>> stream2 = stream
                .map(event -> Tuple2.of(event.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        stream2.print("stream2");

        /**
         * 类型声明方式 2  TypeHint
         */
        DataStream<Tuple2<String, Long>> stream3 = stream
                .map(event -> Tuple2.of(event.user, 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                });
        stream3.print("stream3");

        /**
         * flatMap 使用 Lambda 表达式，必须通过 returns 明确声明返回类型
         * 对于像 flatMap() 这样的函数，它的函数签名 void flatMap(IN value, Collector<OUT> out)
         * 被 Java 编译器编译成了 void flatMap(IN value, Collector out)，
         * 也就是说将 Collector 的泛型信息擦除掉了。这样 Flink 就无法自动推断输出的类型信息了。
         */
        DataStream<String> stream4 = stream.flatMap((Event event, Collector<String> out) -> {
            out.collect(event.url);
        }).returns(Types.STRING);

        stream4.print("stream4");


        env.execute();
    }
}
