package com.cw.flink.chapter05datastream.transformation;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @description:
 * @author:chenwei
 * @date:2022/9/2 10:59
 */
public class PartitionTest {
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

        /*
            1.随机分区（shuffle）
            随机分区服从均匀分布（uniform distribution），所以可以把流中的数据随机打乱，均匀地传递到下游任务分区
         */
        //stream.shuffle().print().setParallelism(4);

        /*
            2.轮询分区
            轮询也是一种常见的重分区方式。简单来说就是“发牌”，按照先后顺序将数据做依次分发，
            通过调用 DataStream 的.rebalance()方法，就可以实现轮询重分区
            rebalance 使用的是Round-Robin 负载均衡算法，可以将输入流数据平均分配到下游的并行任务中去。
         */
        //stream.rebalance().print().setParallelism(4);

        // 默认就是轮询
        //stream.print().setParallelism(4);

        /*
            3.重缩放分区（rescale）
            重缩放分区和轮询分区非常相似。当调用 rescale()方法时，其实底层也是使用Round-Robin算法进行轮询，
            但是只会将数据轮询发送到下游并行任务的一部分中
            也就是说，“发牌人”如果有多个，那么 rebalance 的方式是每个发牌人都面向所有人发牌；
            而 rescale的做法是分成小团体，发牌人只给自己团体内的所有人轮流发牌。
         */
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            // 将奇数发送到索引为 1 的并行子任务
                            // 将偶数发送到索引为 0 的并行子任务
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                sourceContext.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(2)
//                .rescale()
//                .print()
                .setParallelism(4);


        /*
            4.广播
            这种方式其实不应该叫做“重分区”，因为经过广播之后，数据会在不同的分区都保留一份，可能进行重复处理。
            可以通过调用 DataStream 的broadcast()方法，将输入数据复制并发送到下游算子的所有并行任务中去。
         */
        //stream.broadcast().print("broadcast").setParallelism(4);

        /*
            5.全局分区global
            全局分区也是一种特殊的分区方式。这种做法非常极端，通过调用.global()方法，会将所
            有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行度变成了1，
            所以使用这个操作需要非常谨慎，可能对程序造成很大的压力。
         */
//        stream.global().print("global").setParallelism(4);

        /*
            6.自定义重分区
         */
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).print().setParallelism(4);

        env.execute();

    }
}
