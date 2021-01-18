package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-18 9:40
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //2
        DataSource<String> input = executionEnvironment.readTextFile("input");

        input.flatMap(new MyFlatMap()).map(s-> Tuple2.of(s,1L)).returns(Types.TUPLE(Types.STRING,Types.LONG)).groupBy(0).sum(1).print();
    }
    public static class MyFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] strs = value.split(" ");

            for (String str : strs) {
                out.collect(str);
            }
        }
    }
}
