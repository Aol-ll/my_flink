package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-22 10:57
 */
public class Flink01_Project_PV_WC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> textFileDS = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<Tuple2<String,Integer>> userBDS = textFileDS.flatMap((String line, Collector<Tuple2<String,Integer>> userbean) -> {
            String[] split = line.split(",");
            UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
            if ("pv".equals(userBehavior.getBehavior())) {
                userbean.collect(Tuple2.of("pv",1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT));

        userBDS.keyBy(key -> key.f0).sum(1).print();


        env.execute();
    }
}
