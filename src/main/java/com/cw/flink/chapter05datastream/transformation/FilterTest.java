package com.cw.flink.chapter05datastream.transformation;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取操作
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 1.传入一个实现了FilterFunction类的对象
        /**
         * 对于大部分操作而言，都需要传入一个用户自定义函数（UDF），实现相关操作的接口，来完成处理逻辑的定义。
         * Flink 暴露了所有UDF 函数的接口，具体实现方式为接口或者抽象类， 例如 MapFunction、FilterFunction、ReduceFunction 等。
         */
        SingleOutputStreamOperator<Event> result1 = stream.filter(new MyFilter());

        // 2.传入一个匿名类实现FilterFunction接口
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return "Bob".equals(event.user);
            }
        });

        // 3.传入lambda表达式
        SingleOutputStreamOperator<Event> result3 = stream.filter(data -> "Alice".equals(data.user));

        result1.print("result1");
        result2.print("result2");
        result3.print("result3");

        env.execute();

    }

    private static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return "Mary".equals(event.user);
        }
    }
}
