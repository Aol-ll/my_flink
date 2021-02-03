package com.atguigu.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Aol
 * @create 2021-02-03 13:27
 */
public class FlinkSQL04_EventTime_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE source_sensor (" +
                "id string," +
                "ts bigint, " +
                "vc int, " +
                "rt as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rt AS rt - INTERVAL '5' SECOND) WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'SensorSource'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "  'properties.group.id' = 'testGroup1'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")");
        Table table = tableEnv.from("source_sensor");

        table.printSchema();
    }
}
