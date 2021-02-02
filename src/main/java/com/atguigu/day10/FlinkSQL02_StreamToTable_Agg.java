package com.atguigu.day10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Aol
 * @create 2021-02-02 10:49
 */
public class FlinkSQL02_StreamToTable_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[1], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorDS);

        Table select = table.where($("vc").isGreaterOrEqual(40))
                .groupBy($("id"))
                .aggregate($("vc").avg().as("vc_avg"))
                .select($("id"), $("vc_avg"));
/*
        Table select = table.where("vc>=40")
                .groupBy("id")
                .select("id,vc.sum");*/

        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(select, Row.class);

        rowDataStream.print();

        env.execute();
    }
}
