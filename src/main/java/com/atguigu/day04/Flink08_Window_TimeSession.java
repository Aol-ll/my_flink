package com.atguigu.day04;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-22 18:20
 */
public class Flink08_Window_TimeSession {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream
                = streamSource.flatMap((String line, Collector<Tuple2<String, Integer>> word) -> {
            String[] split = line.split(" ");

            for (String s : split) {
                word.collect(Tuple2.of(s, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(key -> key.f0);

        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).sum(1).print();

        env.execute();
    }
}
