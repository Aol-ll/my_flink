package com.atguigu.day11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Aol
 * @create 2021-02-03 13:18
 */
public class FlinkSQL03_EventTime_StreamToTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getTs() * 1000L);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                }).assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("rt").rowtime());

        table.printSchema();
    }
}
