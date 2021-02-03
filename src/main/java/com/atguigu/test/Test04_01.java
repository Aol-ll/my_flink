package com.atguigu.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author Aol
 * @create 2021-02-03 8:54
 */
public class Test04_01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "202102031001");

        DataStreamSource<String> kfDs = env.addSource(new FlinkKafkaConsumer<String>(
                "SensorSource",
                new SimpleStringSchema(),
                properties
        ));

        SingleOutputStreamOperator<WordCount> flatMap = kfDs.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(new WordCount(s));
                }
            }
        });


        tableEnv.createTemporaryView("wc",flatMap);



        tableEnv.executeSql("CREATE TABLE sink_sensor (id string, ct bigint ,PRIMARY KEY (id) NOT ENFORCED) WITH (" +
                "  'connector' = 'elasticsearch-6'," +
                "  'hosts' = 'http://hadoop102:9200'," +
                "  'index' = 'sensor1002'," +
                "  'document-type' = '_doc'," +
                "  'document-id.key-delimiter' = '-'," +
                "  'sink.bulk-flush.max-actions' = '1'" +
                ")");

        tableEnv.executeSql("insert into sink_sensor select word,count(*) from wc group by word");
    }
}
