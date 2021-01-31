package com.atguigu.day08;



import com.atguigu.bean.AdCount;
import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

/**
 * @author Aol
 * @create 2021-01-30 12:47
 */
public class Flink03_Practice_AdCount {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //指定eventTime时间字段
        WatermarkStrategy<AdsClickLog> adsClickLogWatermarkStrategy = WatermarkStrategy.<AdsClickLog>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getTimestamp() * 1000L);

        //转换数据结构为Javabean
        SingleOutputStreamOperator<AdsClickLog> adsClickLogDS = env.readTextFile("input/AdClickLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new AdsClickLog(Long.parseLong(split[0]), Long.parseLong(split[1]), split[2], split[3], Long.parseLong(split[4]));
                }).assignTimestampsAndWatermarks(adsClickLogWatermarkStrategy);

        //过滤要加入黑名单的数据，并输入测输出流
        SingleOutputStreamOperator<AdsClickLog> process = adsClickLogDS.keyBy(key -> key.getUserId() + "_" + key.getAdId())
                .process(new KeyedProcessFunction<String, AdsClickLog, AdsClickLog>() {

                    private ValueState<Long> max_ads;
                    private ValueState<Boolean> isflag;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        max_ads = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ads", Long.class));
                        isflag = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isflag", Boolean.class));
                    }

                    @Override
                    public void processElement(AdsClickLog value, Context ctx, Collector<AdsClickLog> out) throws Exception {

                        Long aLong = max_ads.value();
                        Boolean aBoolean = isflag.value();
                        if (aLong == null) {
                            max_ads.update(1L);

                            Long ts = (value.getTimestamp() / (60 * 60 * 24) + 1) * 60 * 60 * 24 * 1000L - 60 * 60 * 8 * 1000L;
                            ctx.timerService().registerEventTimeTimer(ts);
                        } else {
                            aLong += 1;
                            max_ads.update(aLong);

                            if (aLong > 100) {
                                if (aBoolean == null) {

                                    ctx.output(new OutputTag<String>("sd"){},value.getUserId() + "点击了" + value.getAdId() + "广告达到100次,存在恶意点击广告行为,报警！");
                                    isflag.update(true);
                                }
                            }
                        }
                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdsClickLog> out) throws Exception {
                        max_ads.clear();
                        isflag.clear();
                    }
                });

        //按省份分组，并开窗
        SingleOutputStreamOperator<AdCount> aggregate = process.keyBy(AdsClickLog::getProvince)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AggregateFunction<AdsClickLog, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(AdsClickLog value, Integer accumulator) {
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
                }, new ProcessWindowFunction<Integer, AdCount, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<Integer> elements, Collector<AdCount> out) throws Exception {

                        Integer count = elements.iterator().next();
                        long end = context.window().getEnd();
                        out.collect(new AdCount(s, end, count));
                    }
                });

        //输出结果
        aggregate.print("main");
        process.getSideOutput(new OutputTag<String>("sd"){}).print("sid");
        //执行任务

        env.execute();
    }
}
