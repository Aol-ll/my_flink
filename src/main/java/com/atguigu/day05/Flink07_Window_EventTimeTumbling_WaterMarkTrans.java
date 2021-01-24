package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author Aol
 * @create 2021-01-23 16:17
 */
public class Flink07_Window_EventTimeTumbling_WaterMarkTrans {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<String> stringWatermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((e, s) -> Long.parseLong(e.split(",")[1]) * 1000L);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(stringWatermarkStrategy);

        SingleOutputStreamOperator<WaterSensor> map = stringSingleOutputStreamOperator.map(s -> {
            String[] split = s.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        map.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("vc")
                .print();

        env.execute();

    }
}
