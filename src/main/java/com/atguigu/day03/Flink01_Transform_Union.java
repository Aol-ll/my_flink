package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Aol
 * @create 2021-01-20 10:42
 */
public class Flink01_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource1 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> streamSource2 = env.socketTextStream("hadoop102", 9999);

        DataStream<String> union = streamSource1.union(streamSource2);

        union.print();

        env.execute();

    }
}
