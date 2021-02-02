package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Aol
 * @create 2021-02-02 17:45
 */
public class FlinkSQL12_SQL_MySQL {
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
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://hadoop102:3306/test'," +
                "   'table-name' = 'sensor0821'," +
                "   'username' = 'root'," +
                "   'password' = '123456'" +
                ")");

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor");
    }
}
