package com.atguigu.day07;


import com.atguigu.bean.PageViewCount;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Aol
 * @create 2021-01-29 11:17
 */
public class Flink02_Practice_PageView_Window2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                }).filter(data -> "pv".equals(data.getBehavior())).assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        //转换key添加随机数
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2Ds= userBehaviorDS.map(data -> Tuple2.of("pv_" + new Random().nextInt(8), 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        //按key分组开窗
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = tuple2Ds.keyBy(key -> key.f0).window(TumblingEventTimeWindows.of(Time.hours(1)));

        //组内相加结果转换为PageViewCount
        SingleOutputStreamOperator<PageViewCount> aggregate = window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
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
        }, new ProcessWindowFunction<Integer, PageViewCount, String, TimeWindow>() {

            @Override
            public void process(String s, Context context, Iterable<Integer> elements, Collector<PageViewCount> out) throws Exception {
                Integer count = elements.iterator().next();
                long end = context.window().getEnd();
                out.collect(new PageViewCount("pv", end, count));
            }
        });

        //按窗口时间分组，组内用ListState计算
 /*       aggregate.keyBy(PageViewCount::getTime)
                .process(new KeyedProcessFunction<Long, PageViewCount, Tuple2<String, Integer>>() {
                    private ListState<PageViewCount> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("list", PageViewCount.class));
                    }

                    @Override
                    public void processElement(PageViewCount value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        listState.add(value);
                        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                        int count = 0;
                        Iterator<PageViewCount> iterator = listState.get().iterator();

                        while (iterator.hasNext()) {
                            count += iterator.next().getCount();
                        }

                        Timestamp ts = new Timestamp(ctx.getCurrentKey());
                        out.collect(Tuple2.of(ts.toString(),count));

                        listState.clear();

                    }
                }).print();*/
        aggregate.keyBy(PageViewCount::getTime)
                .process(new KeyedProcessFunction<Long, PageViewCount, Tuple2<String, Integer>>() {
                    private ValueState<PageViewCount> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getState(new ValueStateDescriptor<PageViewCount>("value", PageViewCount.class));
                    }

                    @Override
                    public void processElement(PageViewCount value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer count = value.getCount();
                        if (listState.value() != null) {
                            count += listState.value().getCount();
                        }
                        listState.update(new PageViewCount(value.getPv(),value.getTime(),count));
                        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                        Timestamp ts = new Timestamp(ctx.getCurrentKey());
                        out.collect(Tuple2.of(ts.toString(),listState.value().getCount()));

                        listState.clear();

                    }
                }).print();

        env.execute();
    }
}
