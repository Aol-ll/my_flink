package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Aol
 * @create 2021-01-20 11:02
 */
public class Flink02_Transform_Max {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource
                .map(s -> new WaterSensor(s.split(",")[0], Long.parseLong(s.split(",")[1]), Integer.parseInt(s.split(",")[2])));

        SingleOutputStreamOperator<WaterSensor> max = map.keyBy(WaterSensor::getId).maxBy("vc",false);

        max.print();

        env.execute();
    }
}
