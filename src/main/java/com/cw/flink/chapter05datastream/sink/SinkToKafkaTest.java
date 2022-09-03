package com.cw.flink.chapter05datastream.sink;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author 陈小哥cw
 * @date 2022/9/3 10:48
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 1.从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>(
                "clicks",
                new SimpleStringSchema(),
                properties
        ));

        // 2.用flink进行处理转换
        SingleOutputStreamOperator<String> result = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] fields = value.split(",");
                System.out.println(new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString());
                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
            }
        });

        // 3.结果数据写入kafka
        result.addSink(new FlinkKafkaProducer<String>("hadoop102:9092", "events", new SimpleStringSchema()));


        env.execute();

    }
}
