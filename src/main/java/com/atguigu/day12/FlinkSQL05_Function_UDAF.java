package com.atguigu.day12;

import com.atguigu.bean.SumCount;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author Aol
 * @create 2021-02-04 13:16
 */
public class FlinkSQL05_Function_UDAF {
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

        tableEnv.createTemporarySystemFunction("myudaf", WeightedAvg.class);

        //用table测试
//        table.groupBy($("id")).select($("id"),call("myudaf",$("vc"))).execute().print();

        //用sql测试
        tableEnv.sqlQuery("select id,myudaf(vc) from " + table + " group by id").execute().print();

        //执行任务
        env.execute();
    }

    public static class WeightedAvg extends AggregateFunction<Double, SumCount> {
        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }

        @Override
        public Double getValue(SumCount accumulator) {
            return accumulator.getVcSum() * 1D / accumulator.getCount();
        }

        public void accumulate(SumCount acc, Integer vc) {
            acc.setCount(acc.getCount() + 1);
            acc.setVcSum(acc.getVcSum() + vc);
        }
    }
}
