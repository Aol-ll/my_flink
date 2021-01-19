package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Aol
 * @create 2021-01-19 14:50
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_001", 1577844001L, 45)
        );

        DataStreamSource<WaterSensor> streamSource = env.fromCollection(waterSensors);

        streamSource.print();

        env.execute();
    }
}
