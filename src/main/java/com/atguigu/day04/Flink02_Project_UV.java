package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Aol
 * @create 2021-01-22 12:08
 */
public class Flink02_Project_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> flatMap = streamSource.flatMap((String line, Collector<UserBehavior> word) -> {
            String[] split = line.split(",");
            UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
            if ("pv".equals(userBehavior.getBehavior())) {
                word.collect(userBehavior);
            }
        }).returns(UserBehavior.class);

        KeyedStream<UserBehavior, String> keyedStream = flatMap.keyBy(key -> "uv");

        keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {

            private HashSet<String> hashSet = new HashSet<>();

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                if (hashSet.add(value.getUserId().toString())){

                    out.collect(hashSet.size());
                }
            }
        }).print();

        env.execute();
    }
}
