package com.atguigu.day04;


import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.awt.*;
import java.util.ArrayList;

/**
 * @author Aol
 * @create 2021-01-22 17:07
 */
public class Flink06_Window_TimeTumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream =
                socketTextStream.flatMap((String line, Collector<Tuple2<String, Integer>> word) -> {
                    String[] split = line.split(" ");
                    for (String s : split) {
                        word.collect(Tuple2.of(s, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(key -> key.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream
                = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5L)));

//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);

        //增量累加
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
//
//
//            @Override
//            public Integer createAccumulator() {
//                return 0;
//            }
//
//            @Override
//            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
//                return value.f1 + accumulator;
//            }
//
//            @Override
//            public Integer getResult(Integer accumulator) {
//                return accumulator;
//            }
//
//            @Override
//            public Integer merge(Integer a, Integer b) {
//                return a + b;
//            }
//        }, new WindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>() {
//            @Override
//            public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//
//                Integer next = input.iterator().next();
//                out.collect(Tuple2.of(window + ":" + s, next));
//            }
//        });


//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
//            @Override
//            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                ArrayList<Tuple2<String, Integer>> tuple2s = Lists.newArrayList(input.iterator());
//                out.collect(Tuple2.of(s, tuple2s.size()));
//            }
//        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                ArrayList<Tuple2<String, Integer>> tuple2s = Lists.newArrayList(elements.iterator());

                out.collect(Tuple2.of(context.window() + ":" + s, tuple2s.size()));
            }
        });


        sum.print();
        env.execute();
    }
}
