package com.atguigu.day12;

import com.atguigu.bean.VcTop2;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author Aol
 * @create 2021-02-04 13:27
 */
public class FlinkSQL06_Function_UDTAF {
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

        tableEnv.createTemporarySystemFunction("myudtaf", Top2.class);

        //用table测试
        table.groupBy($("id"))
                .flatAggregate(call("myudtaf", $("vc")).as("topOne", "topTwo"))
                .select($("id"),$("topOne"),$("topTwo")).execute().print();


        //执行任务
        env.execute();
    }
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, String>, VcTop2> {
        @Override
        public VcTop2 createAccumulator() {
            return new VcTop2(Integer.MIN_VALUE,Integer.MIN_VALUE);
        }

        public void accumulate(VcTop2 acc, Integer value) {

            if (value > acc.getTopOne()){
                acc.setTopTwo(acc.getTopOne());
                acc.setTopOne(value);

            }else if (value >acc.getTopTwo()){
                acc.setTopTwo(value);
            }

        }
        public void emitValue(VcTop2 acc, Collector<Tuple2<Integer, String>> out){
            out.collect(Tuple2.of(acc.getTopOne(),"1"));
            if (acc.getTopTwo() != Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.getTopTwo(),"2"));
            }
        }
    }

    }
