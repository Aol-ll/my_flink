package com.atguigu.day10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Aol
 * @create 2021-02-02 15:10
 */
public class FlinkSQL07_Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table select = tableEnv.fromDataStream(waterSensorDS)
                .select($("id"), $("ts"), $("vc"));


        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("hadoop102",9200,"http")
                .index("mySensor")
                .documentType("_doc")
                .bulkFlushMaxActions(1))
                .withSchema(new Schema()
                      .field("id", DataTypes.STRING())
                      .field("ts", DataTypes.BIGINT())
                      .field("vc", DataTypes.INT()))
                .withFormat(new Json())
                .inAppendMode()
                .createTemporaryTable("sensor_water");

        select.executeInsert("sensor_water");

        env.execute();
    }
}
