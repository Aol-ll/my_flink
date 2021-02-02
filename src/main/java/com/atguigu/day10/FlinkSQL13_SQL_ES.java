package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Aol
 * @create 2021-02-02 18:03
 */
public class FlinkSQL13_SQL_ES {
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
        tableEnv.executeSql("CREATE TABLE sink_sensor (id string, ts bigint, vc int ,PRIMARY KEY (id) NOT ENFORCED) WITH (" +
                "  'connector' = 'elasticsearch-6'," +
                "  'hosts' = 'http://hadoop102:9200'," +
                "  'index' = 'sensor0821'," +
                "  'document-type' = '_doc'," +
                "  'document-id.key-delimiter' = '-'," +
                "  'sink.bulk-flush.max-actions' = '1'" +
                ")");

        tableEnv.executeSql("insert into sink_sensor select id,count(*) ts,sum(vc) vc from source_sensor group by id");

    }
}
