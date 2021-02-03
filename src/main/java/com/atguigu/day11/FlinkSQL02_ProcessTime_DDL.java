package com.atguigu.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Aol
 * @create 2021-02-03 13:06
 */
public class FlinkSQL02_ProcessTime_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE source_sensor (" +
                "id string," +
                "ts bigint, " +
                "vc int, " +
                "pt as proctime()) WITH (" +
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
