package com.atguigu.day11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author Aol
 * @create 2021-02-03 13:41
 */
public class FlinkSQL05_TableAPI_GroupWindow_TumblingWindow_ProcessTime {
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
                .window(Tumble.over(lit(5).seconds()).on($("pt")).as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count());

        DataStream<Row> dataStream = tableEnv.toAppendStream(select, Row.class);

        dataStream.print();

        env.execute();


    }
}
