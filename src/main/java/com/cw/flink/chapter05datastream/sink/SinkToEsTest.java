package com.cw.flink.chapter05datastream.sink;

import com.cw.flink.chapter05datastream.source.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author 陈小哥cw
 * @date 2022/9/3 11:54
 */
public class SinkToEsTest {
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

        // 定义host列表
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

        // 定义ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> map = new HashMap<>();
                map.put(event.user, event.url);

                // 构建一个IndexRequest
                IndexRequest request = Requests.indexRequest()
                        .index("clicks")
                        .type("type")
                        .source(map);

                requestIndexer.add(request);
            }
        };

        // 写入ES
        stream.addSink(new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction).build());

        env.execute();

    }
}
