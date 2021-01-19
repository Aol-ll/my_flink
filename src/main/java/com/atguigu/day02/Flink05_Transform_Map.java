package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Aol
 * @create 2021-01-19 18:24
 */
public class Flink05_Transform_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");

        ds.map(line->{
            String[] split = line.split(",");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        }).print();

        env.execute();
    }
}
