package com.atguigu.day12;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author Aol
 * @create 2021-02-04 13:06
 */
public class FlinkSQL04_Function_UDTF {
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

        tableEnv.createTemporarySystemFunction("myudtf",SplitFunction.class);

        //用table测试
   /*     table.joinLateral(call("myudtf",$("id")))
                .select($("id"),$("word"),$("length")).execute().print();*/

        //用sql测试
        tableEnv.sqlQuery("select id,word,length from " + table + ",lateral table(myudtf(id))").execute().print();

        //执行任务
        env.execute();
    }
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            for (String s : str.split("_")) {

                collect(Row.of(s, s.length()));
            }
        }
    }
}
