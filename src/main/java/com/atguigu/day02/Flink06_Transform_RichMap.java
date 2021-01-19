package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Aol
 * @create 2021-01-19 18:29
 */
public class Flink06_Transform_RichMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<WaterSensor> map = ds.map(new MyMapFunc());

        map.print();
        env.execute();

    }
    public static class MyMapFunc extends RichMapFunction<String, WaterSensor>{
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("666");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("停停停");
        }
    }
}
