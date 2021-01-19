package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

/**
 * @author Aol
 * @create 2021-01-19 18:44
 */
public class Flink09_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> input1 = env.socketTextStream("hadoop102",9999);
        DataStreamSource<String> input2 = env.socketTextStream("hadoop102", 8888);

        SingleOutputStreamOperator<Integer> map1 = input2.map(String::length);

        ConnectedStreams<String, Integer> connect = input1.connect(map1);

        SingleOutputStreamOperator<Object> map = connect.map(new MyConnectFunc());

        map.print();

        env.execute();
    }
    public static class MyConnectFunc extends RichCoMapFunction<String,Integer,Object>{


        @Override
        public String map1(String value) throws Exception {
            return value;
        }

        @Override
        public Integer map2(Integer value) throws Exception {
            return value;
        }
    }
}
