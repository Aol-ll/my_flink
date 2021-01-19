package com.atguigu.day02;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-19 18:36
 */
public class Flink07_Transform_RichFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<String> f = ds.flatMap(new MyFlatMap());

        f.print();

        env.execute();
    }

    public static class MyFlatMap extends RichFlatMapFunction<String,String>{
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("666");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {

            if (Integer.parseInt(value.split(",")[2])>30){
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("停停停");
        }
    }
}
