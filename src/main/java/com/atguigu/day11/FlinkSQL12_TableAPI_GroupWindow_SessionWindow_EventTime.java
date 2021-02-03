package com.atguigu.day11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author Aol
 * @create 2021-02-03 15:02
 */
public class FlinkSQL12_TableAPI_GroupWindow_SessionWindow_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((e, s) -> e.getTs() * 1000L);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                }).assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table select = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("rt").rowtime())
                .window(Session.withGap(lit(5).seconds()).on($("rt")).as("rw"))
                .groupBy($("id"), $("rw"))
                .select($("id"), $("id").count());

        tableEnv.toAppendStream(select, Row.class).print();

        env.execute();
    }
}
