package com.cw.flink.chapter05datastream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件中读取数据
        /*
            1.参数可以是目录，也可以是文件
            2.路径可以是相对路径，也可以是绝对路径
            3.相对路径是从系统属性 user.dir 获取路径: idea 下是 project 的根目录, standalone 模式下是集群节点根目录
            4.也可以从hdfs 目录下读取, 使用路径hdfs://..., 由于 Flink 没有提供hadoop 相关依赖, 需要 pom 中添加相关依赖
         */
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.csv");

        // 2.从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3.从元素读取数据，直接将元素列举出来，调用 fromElements 方法进行读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 4.从socket文本流中获取
        DataStreamSource<String> stream4 = env.socketTextStream("10.13.33.126", 7777);


//        stream1.print("1");
//        numStream.print("nums");
//        stream2.print("2");
//        stream3.print("3");
        stream4.print("4");

        env.execute();
    }
}
