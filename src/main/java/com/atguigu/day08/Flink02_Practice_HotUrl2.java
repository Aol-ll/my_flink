package com.atguigu.day08;

import com.atguigu.bean.ApacheLog;
import com.atguigu.bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Aol
 * @create 2021-01-30 10:39
 */
public class Flink02_Practice_HotUrl2 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //指定eventtime时间字段
        WatermarkStrategy<ApacheLog> apacheLogWatermarkStrategy = WatermarkStrategy.<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((e, s) -> e.getTs());

        //转换数据类型转化为javabean
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(" ");
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    return new ApacheLog(split[0], split[1], sdf.parse(split[3]).getTime(), split[5], split[6]);
                }).filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(apacheLogWatermarkStrategy);

        //按url分组然后开窗,并计算
        SingleOutputStreamOperator<UrlCount> aggregate = apacheLogDS.keyBy(ApacheLog::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<ApacheLog>("sid") {
                })
                .aggregate(new AggregateFunction<ApacheLog, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(ApacheLog value, Integer accumulator) {
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
                }, new ProcessWindowFunction<Integer, UrlCount, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Integer> elements, Collector<UrlCount> out) throws Exception {
                        long end = context.window().getEnd();
                        Integer count = elements.iterator().next();
                        out.collect(new UrlCount(key, end, count));
                    }
                });

        //按窗口时间分组,并计算
        SingleOutputStreamOperator<String> process = aggregate.keyBy(UrlCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, UrlCount, String>() {

                    private Integer topN = 5;
                    private MapState<String, UrlCount> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlCount>("map", String.class,UrlCount.class));
                    }

                    @Override
                    public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
                        mapState.put(value.getUrl(),value);
                        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
                        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 61001L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        if (timestamp == (ctx.getCurrentKey() + 61001L)) {
                            mapState.clear();
                            return;
                        }
                        Iterator<Map.Entry<String, UrlCount>> iterator = mapState.iterator();

                        ArrayList<Map.Entry<String, UrlCount>> urlCounts = Lists.newArrayList(iterator);

                        urlCounts.sort(((o1, o2) -> o2.getValue().getCount() - o1.getValue().getCount()));

                        StringBuilder sb = new StringBuilder();
                        sb.append("===========")
                                .append(new Timestamp(timestamp - 1L))
                                .append("===========")
                                .append("\n");
                        for (int i = 0; i < Math.min(urlCounts.size(), topN); i++) {
                            UrlCount urlCount = urlCounts.get(i).getValue();
                            sb.append("Top").append(i + 1);
                            sb.append(" Url:").append(urlCount.getUrl());
                            sb.append(" Count:").append(urlCount.getCount());
                            sb.append("\n");
                        }
                        sb.append("===========")
                                .append(new Timestamp(timestamp - 1L))
                                .append("===========")
                                .append("\n")
                                .append("\n");

                        out.collect(sb.toString());


                        Thread.sleep(2000);
                    }
                });

        //打印结果
        process.print();
        //执行任务
        env.execute();

    }
}
