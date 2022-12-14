package com.cw.flink.chapter12_cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: chenwei
 * @date: 2022/9/28 16:34
 */
public class LoginFailDetectProExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.获取登录数据流
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 2.定义模式，连续三次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")// 第一次登录失败事件
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.eventType);
                    }
                }).times(3).consecutive();// 指定是严格近邻的三次登录失败

        // 3.将模式应用到数据流上，检测复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(event -> event.userId), pattern);

        // 4.将检测到的复杂事件提取出来，进行处理得到报警信息输出
        SingleOutputStreamOperator<String> warningStream = patternStream.process(new PatternProcessFunction<LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> map, Context context, Collector<String> collector) throws Exception {
                // 提取三次登录失败事件
                LoginEvent firstFailEvent = map.get("fail").get(0);
                LoginEvent secondFailEvent = map.get("fail").get(1);
                LoginEvent thirdFailEvent = map.get("fail").get(2);

                collector.collect(
                        firstFailEvent.userId + " 连续三次登录失败！登录时间："
                                + firstFailEvent.timestamp + ","
                                + secondFailEvent.timestamp + ","
                                + thirdFailEvent.timestamp
                );
            }
        });

        // 打印输出
        warningStream.print();

        env.execute();


    }
}
