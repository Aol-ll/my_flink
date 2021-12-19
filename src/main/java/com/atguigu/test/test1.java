package com.atguigu.test;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-03-25 9:17
 */
public class test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource =
                env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String,Integer>> wordToOneDS = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (int i = 0; i < split.length; i++) {
                    out.collect(Tuple2.of(split[i],1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordToOneDS.keyBy(str -> str.f0).sum(1);

        result.print();

        env.execute();
    }
}
