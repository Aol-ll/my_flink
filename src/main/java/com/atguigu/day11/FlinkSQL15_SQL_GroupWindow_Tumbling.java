package com.atguigu.day11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Aol
 * @create 2021-02-03 17:05
 */
public class FlinkSQL15_SQL_GroupWindow_Tumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());



        Table result = tableEnv.sqlQuery("select " +
                "id," +
                "count(id)," +
                "TUMBLE_START(pt, INTERVAL '5' second) as wStart from " +
                table +
                " group by id,TUMBLE(pt, INTERVAL '5' second)");

        result.execute().print();

        env.execute();

    }
}
