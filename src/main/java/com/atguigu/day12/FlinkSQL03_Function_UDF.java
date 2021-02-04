package com.atguigu.day12;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author Aol
 * @create 2021-02-04 12:50
 */
public class FlinkSQL03_Function_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorDS);

        //直接使用不注册
//        table.select($("id"), call(myUDF.class, $("id"))).execute().print();

        //注册函数用table运行
        tableEnv.createTemporarySystemFunction("myudf",myUDF.class);
//        table.select($("id"), call("myudf", $("id"))).execute().print();
        //sql运行
        tableEnv.sqlQuery("select id,myudf(id) from " + table).execute().print();

        env.execute();


    }
    public static class myUDF extends ScalarFunction{

        public int eval(String s) {
            return s.length();
        }
    }
}
