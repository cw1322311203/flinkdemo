package com.cw.flink.chapter05datastream.transformation;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @description: “分区”（partitioning）操作就是要将数据进行重新分布，传递到不同的流分区去进行下一步处理。keyBy就是一种按照键的哈希值来进行重新分区的操作。
 * 只不过这种分区操作只能保证把数据按key“分开”，至于分得均不均匀、每个 key 的数据具体会分到哪一区去，这些是完全无从控制的。
 * 所以我们有时也说，keyBy 是一种逻辑分区（logical partitioning）操作。
 * 物理分区与keyBy 另一大区别在于，keyBy 之后得到的是一个KeyedStream，而物理分区之后结果仍是DataStream，且流中元素数据类型保持不变。
 * 从这一点也可以看出，分区算子并不对数据进行转换处理，只是定义了数据的传输方式。
 * @author:chenwei
 * @date:2022/9/2 10:59
 */
public class PartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
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
         1.随机分区（shuffle）
         随机分区服从均匀分布（uniform distribution），所以可以把流中的数据随机打乱，均匀地传递到下游任务分区
         因为是完全随机的，所以对于同样的输入数据, 每次执行得到的结果也不会相同。
         经过随机分区之后，得到的依然是一个DataStream。
         */
//        stream.shuffle().print("shuffle随机分区").setParallelism(4);

        /**
         2.轮询分区
         轮询也是一种常见的重分区方式。简单来说就是“发牌”，按照先后顺序将数据做依次分发，
         通过调用 DataStream 的.rebalance()方法，就可以实现轮询重分区
         rebalance 使用的是Round-Robin 负载均衡算法，可以将输入流数据平均分配到下游的并行任务中去。
         */
//        stream.rebalance().print("rebalance轮询分区").setParallelism(4);

        // 默认就是轮询
//        stream.print("默认轮询分区").setParallelism(4);

        /**
         3.重缩放分区（rescale）
         重缩放分区和轮询分区非常相似。当调用 rescale()方法时，其实底层也是使用Round-Robin算法进行轮询，
         但是只会将数据轮询发送到下游并行任务的一部分中
         也就是说，“发牌人”如果有多个，那么 rebalance 的方式是每个发牌人都面向所有人发牌；
         而 rescale的做法是分成小团体，发牌人只给自己团体内的所有人轮流发牌。

         当下游任务（数据接收方）的数量是上游任务（数据发送方）数量的整数倍时，rescale 的效率明显会更高。
         比如当上游任务数量是 2，下游任务数量是 6 时，上游任务其中一个分区的数据就将会平均分配到下游任务的 3 个分区中。

         由于 rebalance 是所有分区数据的“重新平衡”，当 TaskManager 数据量较多时，这种跨节点的网络传输必然影响效率；
         而如果我们配置的 task slot 数量合适，用 rescale 的方式进行“局部重缩放”，就可以让数据只在当前 TaskManager 的
         多个 slot 之间重新分配，从而避免了网络传输带来的损耗。

         从底层实现上看，rebalance 和 rescale 的根本区别在于任务之间的连接机制不同。rebalance 将会针对所有上游任务（发送数据方）
         和所有下游任务（接收数据方）之间建立通信通道，这是一个笛卡尔积的关系；
         而 rescale 仅仅针对每一个任务和下游对应的部分任务之间建立通信通道，节省了很多资源。
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
//                .print("rescale分区")
                .setParallelism(4);


        /**
         4.广播
         这种方式其实不应该叫做“重分区”，因为经过广播之后，数据会在不同的分区都保留一份，可能进行重复处理。
         可以通过调用 DataStream 的broadcast()方法，将输入数据复制并发送到下游算子的所有并行任务中去。
         所有数据都会在每个分区保留一份
         */
        // 经广播后打印输出，并行度为 4
//        stream.broadcast().print("broadcast广播").setParallelism(4); // 32条记录

        /**
         5.全局分区global
         全局分区也是一种特殊的分区方式。这种做法非常极端，通过调用.global()方法，会将所
         有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行度变成了1，
         所以使用这个操作需要非常谨慎，可能对程序造成很大的压力。
         */
        stream.global().print("global全局分区").setParallelism(5);

        /**
         6.自定义重分区
         当 Flink 提供的所有分区策略都不能满足用户的需求时， 我们可以通过使用partitionCustom()方法来自定义分区策略。
         在调用时，方法需要传入两个参数，
         第一个是自定义分区器（Partitioner）对象，
         第二个是应用分区器的字段，
         它的指定方式与 keyBy 指定 key 基本一样：可以通过字段名称指定， 也可以通过字段位置索引来指定，还可以实现一个KeySelector。
         */
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                return key % 2;
                            }
                        },
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer value) throws Exception {
                                return value;
                            }
                        }
                )
                .print("自定义重分区")
                .setParallelism(4);

        env.execute();

    }
}
