package com.atguigu.day12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * @author Aol
 * @create 2021-02-04 18:26
 */
public class Flink08_HiveCatalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        HiveCatalog hiveCatalog = new HiveCatalog("hiveCatalog", "default", "input");
        tableEnv.registerCatalog("hiveCatalog",hiveCatalog);
        tableEnv.useCatalog("hiveCatalog");

        tableEnv.executeSql("select * from business").print();
    }
}
