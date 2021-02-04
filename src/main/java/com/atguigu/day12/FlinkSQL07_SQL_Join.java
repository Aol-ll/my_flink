package com.atguigu.day12;

import com.atguigu.bean.TableA;
import com.atguigu.bean.TableB;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Aol
 * @create 2021-02-04 18:05
 */
public class FlinkSQL07_SQL_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        SingleOutputStreamOperator<TableA> TA = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new TableA(split[0],split[1]);
                });
        SingleOutputStreamOperator<TableB> TB = env.socketTextStream("localhost", 8888)
                .map(data -> {
                    String[] split = data.split(",");
                    return new TableB(split[0], Integer.parseInt(split[1]));
                });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        tableEnv.createTemporaryView("tableA",TA);
        tableEnv.createTemporaryView("tableB",TB);

        tableEnv.sqlQuery("select * from tableA join tableB on tableA.id=tableB.id").execute().print();

        env.execute();
    }
}
