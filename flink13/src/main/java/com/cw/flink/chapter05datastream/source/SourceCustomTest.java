package com.cw.flink.chapter05datastream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 注意的是 SourceFunction 接口定义的数据源，并行度只能设置为 1，如果数据源设置为大于 1 的并行度，则会抛出异常。
 * 如果我们想要自定义并行的数据源的话，需要使用 ParallelSourceFunction
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据
        DataStreamSource<Event> customStream = env.addSource(new ClickSource());

        customStream.print();

        env.execute();

    }
}


