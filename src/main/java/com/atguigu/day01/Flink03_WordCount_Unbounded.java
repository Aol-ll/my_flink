package com.atguigu.day01;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author Aol
 * @create 2021-01-18 9:40
 */
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取文件
        DataStreamSource<String> input = env.socketTextStream("hadoop102",9999);
        //3.压平
        input.flatMap((String line, Collector<Tuple2<String, Integer>> words) -> {
            Arrays.stream(line.split(" ")).forEach(word -> words.collect(Tuple2.of(word, 1)));
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(key -> key.f0)
                .sum(1)
                .print();
        //4.执行程序
        env.execute();
    }
}
