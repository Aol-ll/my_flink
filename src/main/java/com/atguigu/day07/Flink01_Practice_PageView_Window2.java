package com.atguigu.day07;

import com.atguigu.bean.PageViewCount;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Aol
 * @create 2021-01-29 11:17
 */
public class Flink01_Practice_PageView_Window2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = env.readTextFile("input/UserBehavior.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                }).filter(data -> "pv".equals(data.getBehavior())).assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = userBehaviorSingleOutputStreamOperator.map(data -> Tuple2.of("PV_" + new Random().nextInt(8), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(key -> key.f0);

        SingleOutputStreamOperator<PageViewCount> aggregate = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PageViewAggFunc(), new PageViewWindowFunc());

//        aggregate.keyBy(PageViewCount::getTime).sum("count").print();

        aggregate.keyBy(PageViewCount::getTime).process(new PageViewProcessFunc()).print();


        env.execute();
    }

    private static class PageViewAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    private static class PageViewWindowFunc implements WindowFunction<Integer, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<PageViewCount> out) throws Exception {

            long end = window.getEnd();
            Integer count = input.iterator().next();
            out.collect(new PageViewCount("pv", end, count));
        }
    }

    private static class PageViewProcessFunc extends KeyedProcessFunction<Long, PageViewCount, Tuple2<String, Integer>> {

        private ListState<Integer> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list", Integer.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            listState.add(value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getTime() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {



            Iterable<Integer> iterable = listState.get();

            Iterator<Integer> iterator = iterable.iterator();

            Integer count = 0;

            while (iterator.hasNext()) {
                count += iterator.next();
            }

            out.collect(Tuple2.of(new Timestamp(timestamp-1).toString(), count));

            listState.clear();


        }
    }
}
