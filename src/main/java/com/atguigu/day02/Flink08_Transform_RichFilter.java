package com.atguigu.day02;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Aol
 * @create 2021-01-19 18:41
 */
public class Flink08_Transform_RichFilter {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<String> filter = ds.filter(new MyFilterFunc());

        filter.print();

        env.execute();
    }
    public static class MyFilterFunc extends RichFilterFunction<String>{
        @Override
        public boolean filter(String value) throws Exception {


            return Integer.parseInt(value.split(",")[2])>30;
        }
    }
}
