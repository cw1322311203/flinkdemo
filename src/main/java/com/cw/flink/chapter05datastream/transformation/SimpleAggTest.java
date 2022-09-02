package com.cw.flink.chapter05datastream.transformation;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:简答聚合算子
 * @author:chenwei
 * @date:2022/9/1 15:45
 * 简答聚合算子
 * sum()：在输入流上，对指定的字段做叠加求和的操作。
 * min()：在输入流上，对指定的字段求最小值。
 * max()：在输入流上，对指定的字段求最大值。
 * minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计算指定字段的最小值，其他字段会保留
 * 最初第一个数据的值；而minBy()则会返回包含字段最小值的整条数据。
 * maxBy() ：与 max() 类似， 在输入流上针对指定字段求最大值。两者区别与min()/minBy()完全一致。
 */
public class SimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取操作
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        // 按键分组之后进行聚合，提取当前用户最近一次访问数据
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("max: ");


        stream.keyBy(data -> data.user).maxBy("timestamp").print("maxBy: ");

        env.execute();


    }
}
