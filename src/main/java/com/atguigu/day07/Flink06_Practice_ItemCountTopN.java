package com.atguigu.day07;


import com.atguigu.bean.ItemCount;
import com.atguigu.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * @author Aol
 * @create 2021-01-29 17:08
 */
public class Flink06_Practice_ItemCountTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getTimestamp() * 1000L);

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        WindowedStream<UserBehavior, Long, TimeWindow> window = userBehaviorDS.keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)));

        window.aggregate(new AggregateFunction<UserBehavior, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(UserBehavior value, Integer accumulator) {
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
        }, new ProcessWindowFunction<Integer, ItemCount, Long, TimeWindow>() {

            @Override
            public void process(Long aLong, Context context, Iterable<Integer> elements, Collector<ItemCount> out) throws Exception {

                Integer next = elements.iterator().next();

                long end = context.window().getEnd();

                String ts = new Timestamp(end).toString();
                out.collect(new ItemCount(aLong, ts, next));
            }
        }).keyBy(ItemCount::getTime)
                .process(new KeyedProcessFunction<String, ItemCount, String>() {

                    private ListState<ItemCount> listState;

                    private int topN = 5;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("list", ItemCount.class));
                    }

                    @Override
                    public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);

                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        ctx.timerService().registerEventTimeTimer(simpleDateFormat.parse(value.getTime()).getTime() + 1000L);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ArrayList<ItemCount> itemCounts = Lists.newArrayList(listState.get().iterator());

                        itemCounts.sort((o1,o2)->o2.getCount()-o1.getCount());

                        StringBuilder sb = new StringBuilder();


                        sb.append("===========")
                                .append(new Timestamp(timestamp - 1000L))
                                .append("===========")
                                .append("\n");
                        for (int i = 0; i < Math.min(itemCounts.size(),topN); i++) {
                            ItemCount itemCount = itemCounts.get(i);
                            sb.append(itemCount.getItem());
                            sb.append(":::");
                            sb.append(itemCount.getCount());
                            sb.append("\n");
                        }

                        sb.append("===========")
                                .append(new Timestamp(timestamp - 1000L))
                                .append("===========")
                                .append("\n")
                                .append("\n");
                        listState.clear();
                        out.collect(sb.toString());
//                        Thread.sleep(2000);
                    }
                })
                .print();

        env.execute();

    }
}
