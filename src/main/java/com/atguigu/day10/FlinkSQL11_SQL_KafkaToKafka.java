package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Aol
 * @create 2021-02-02 17:43
 */
public class FlinkSQL11_SQL_KafkaToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE source_sensor (id string, ts bigint, vc int) WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'SensorSource'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")");

        tableEnv.executeSql("CREATE TABLE sink_sensor (id string, ts bigint, vc int) WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'SensorSink'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "  'format' = 'json'" +
                ")");

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor");
    }
}
