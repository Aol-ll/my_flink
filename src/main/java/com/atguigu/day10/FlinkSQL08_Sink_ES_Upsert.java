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
public class FlinkSQL08_Sink_ES_Upsert {
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
                .groupBy($("id"))
                .select($("id"), $("ts").count().as("ct"), $("vc").sum().as("vc_sum"));

        tableEnv.connect(new Elasticsearch()
                .host("hadoop102",9200,"http")
                .version("6")
                .index("sensor2021")
                .documentType("_doc")
                .bulkFlushMaxActions(1))
                .withSchema(new Schema()
                      .field("id", DataTypes.STRING())
                      .field("ct", DataTypes.BIGINT())
                      .field("vc_sum", DataTypes.INT()))
                .withFormat(new Json())
                .inUpsertMode()
                .createTemporaryTable("sensor");
        select.executeInsert("sensor");

        env.execute();
    }
}
