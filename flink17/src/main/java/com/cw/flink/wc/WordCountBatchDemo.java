package com.cw.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @program: flinkdemo
 * @description DataSet API实现word count（已过时）
 * @author: c28023
 * @create: 2023/8/14 - 15:43
 **/
public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.读取数据：从文件中读取
        DataSource<String> lineDS = env.readTextFile("input/words.txt");

        // TODO 3.切分、转换 (word,1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // TODO 3.1 按照空格切分单词
                String[] words = value.split(" ");
                // TODO 3.2 将单词转换为(word,1)
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    // TODO 3.3 使用Collector向下游发送数据
                    out.collect(wordTuple2);
                }
            }
        });

        // TODO 4.按照word分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);

        // TODO 5.各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupBy.sum(1);// 1是位置，表示第2个元素

        // TODO 6.输出
        sum.print();
    }
}
