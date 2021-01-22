package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
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
public class Flink01_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> textFileDS = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBDS = textFileDS.flatMap((String line, Collector<UserBehavior> userbean) -> {
            String[] split = line.split(",");
            UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
            if ("pv".equals(userBehavior.getBehavior())) {
                userbean.collect(userBehavior);
            }
        }).returns(UserBehavior.class);

        KeyedStream<UserBehavior, String> keyedStream = userBDS.keyBy(key -> "PV");

        keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {

            Integer count = 0;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {

                count++;

                out.collect(count);
            }
        }).print();

        env.execute();
    }
}
