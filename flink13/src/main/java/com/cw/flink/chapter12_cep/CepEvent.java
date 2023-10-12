package com.cw.flink.chapter12_cep;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: chenwei
 * @date: 2022/9/30 9:40
 */
public class CepEvent {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Integer, String, String>> eventStream = env.fromElements(
                Tuple3.of(1500, "login", "fail"),
                Tuple3.of(1500, "login", "fail"),
                Tuple3.of(1500, "login", "fail"),
                Tuple3.of(1320, "login", "success"),
                Tuple3.of(1450, "exit", "success"),
                Tuple3.of(982, "login", "fail"));
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<Tuple3<Integer, String, String>, ?> loginFail = Pattern
                .<Tuple3<Integer, String, String>>begin("begin", skipStrategy)
                .where(new IterativeCondition<Tuple3<Integer, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<Integer, String, String> s,
                                          Context<Tuple3<Integer, String, String>> context) throws Exception {
                        return s.f2.equalsIgnoreCase("fail");
                    }
                })
                .times(3)
                .within(Time.seconds(5));

        PatternStream<Tuple3<Integer, String, String>> patternStream = CEP.pattern(eventStream.keyBy(x -> x.f0), loginFail);
        DataStream<String> alarmStream = patternStream.select(new PatternSelectFunction<Tuple3<Integer, String, String>, String>() {
            @Override
            public String select(Map<String, List<Tuple3<Integer, String, String>>> map) throws Exception {
                String msg = String.format("ID %d has login failed 3 times in 5 seconds.", map.values().iterator().next().get(0).f0);
                return msg;
            }
        });

        alarmStream.print();

        env.execute("cep event test");
    }
}
