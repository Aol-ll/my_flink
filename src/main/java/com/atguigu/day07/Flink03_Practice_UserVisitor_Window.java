package com.atguigu.day07;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author Aol
 * @create 2021-01-29 17:07
 */
public class Flink03_Practice_UserVisitor_Window {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定eventtime窗口中的时间字段
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getTimestamp() * 1000L);

        //转换数据结构
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                }).filter(data->"pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);
        //按key分组并开窗
        WindowedStream<UserBehavior, String, TimeWindow> window = userBehaviorDS.keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.hours(1)));

        window.process(new ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow>() {


            @Override
            public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
                HashSet<Long> uids = new HashSet<>();

                Iterator<UserBehavior> iterator = elements.iterator();

                while (iterator.hasNext()) {
                    uids.add(iterator.next().getUserId());
                }
                long end = context.window().getEnd();

                Timestamp ts = new Timestamp(end);

                out.collect(new UserVisitorCount("uv", ts.toString(), uids.size()));

            }
        }).print();

        env.execute();


    }
}
