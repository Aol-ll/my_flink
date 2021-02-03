package com.atguigu.day11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author Aol
 * @create 2021-02-03 14:42
 */
public class FlinkSQL07_TableAPI_GroupWindow_SessionWindow_ProcessTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table select = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime())
                .window(Session.withGap(lit(5).seconds()).on($("pt")).as("sw"))
                .groupBy($("id"), $("sw"))
                .select($("id"), $("id").count());

        tableEnv.toAppendStream(select, Row.class).print();

        env.execute();
    }
}
