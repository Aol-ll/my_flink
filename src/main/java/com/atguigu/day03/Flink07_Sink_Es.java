package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;

import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Aol
 * @create 2021-01-20 16:25
 */
public class Flink07_Sink_Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(s -> {
            String[] split = s.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        HttpHost httpHost = new HttpHost("hadoop102", 9200);

        ArrayList<HttpHost> list = new ArrayList<>();
        list.add(httpHost);
        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(list, (waterSensor, runtimeContext, requestIndexer) -> {
            HashMap<String, String> hashMap = new HashMap<>();
            hashMap.put("ts", waterSensor.getTs().toString());
            hashMap.put("vc", waterSensor.getVc().toString());

            IndexRequest source = Requests.indexRequest()
                    .index("sonser")
                    .type("_doc")
                    .id(waterSensor.getId())
                    .source(hashMap);
            requestIndexer.add(source);
        });

        waterSensorBuilder.setBulkFlushMaxActions(1);

        map.addSink(waterSensorBuilder.build());

        env.execute();
    }
}
