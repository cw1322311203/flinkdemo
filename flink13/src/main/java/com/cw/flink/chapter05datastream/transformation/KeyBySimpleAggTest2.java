package com.cw.flink.chapter05datastream.transformation;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: keyBy 是聚合前必须要用到的一个算子。keyBy 通过指定键（key），可以将一条流从逻辑上
 * 划分成不同的分区（partitions）。这里所说的分区，其实就是并行处理的子任务，也就对应着任务槽（task slot）。
 * 在内部，是通过计算 key 的哈希值（hash code），对分区数进行取模运算来实现的。所以这里 key 如果是POJO 的话，必须要重写 hashCode()方法。
 * keyBy()方法需要传入一个参数，这个参数指定了一个或一组 key。有很多不同的方法来指定 key：比如对于 Tuple 数据类型，可以指定字段的位置或者多个位置的组合；
 * 对于 POJO 类型，可以指定字段的名称（String）；另外，还可以传入 Lambda 表达式或者实现一个键选择器（KeySelector），用于说明从数据中提取 key 的逻辑。
 * 简单聚合算子
 * sum()：在输入流上，对指定的字段做叠加求和的操作。
 * min()：在输入流上，对指定的字段求最小值。
 * max()：在输入流上，对指定的字段求最大值。
 * minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计算指定字段的最小值，其他字段会保留最初第一个数据的值；
 * 而minBy()则会返回包含字段最小值的整条数据。
 * maxBy() ：与 max() 类似， 在输入流上针对指定字段求最大值。两者区别与min()/minBy()完全一致。
 * @author: chenwei
 * @date: 2022/9/1 15:45
 */
public class KeyBySimpleAggTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取操作
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

        /**
         * 简单聚合算子使用非常方便，语义也非常明确。这些聚合方法调用时，也需要传入参数；
         * 但并不像基本转换算子那样需要实现自定义函数，只要说明聚合指定的字段就可以了。
         * 指定字段的方式有两种：指定位置，和指定名称。
         *
         * sum(int positionToSum)
         * sum(String field)
         */
        stream.keyBy(r -> r.f0).sum(1).print();
        /**
         * 对于元组类型的数据，同样也可以使用这两种方式来指定字段。需要注意的是，元组中字段的名称，是以 f0、f1、f2、…来命名的。
         */
        stream.keyBy(r -> r.f0).sum("f1").print();
        stream.keyBy(r -> r.f0).max(1).print();
        stream.keyBy(r -> r.f0).max("f1").print();
        stream.keyBy(r -> r.f0).min(1).print();
        stream.keyBy(r -> r.f0).min("f1").print();
        /**
         * maxBy(int positionToMaxBy)
         * maxBy(String field)
         */
        stream.keyBy(r -> r.f0).maxBy(1).print();
        stream.keyBy(r -> r.f0).maxBy("f1").print();
        stream.keyBy(r -> r.f0).minBy(1).print();
        stream.keyBy(r -> r.f0).minBy("f1").print();


        env.execute();


    }
}
