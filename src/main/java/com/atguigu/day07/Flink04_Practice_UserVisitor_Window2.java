package com.atguigu.day07;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * @author Aol
 * @create 2021-01-29 17:08
 */
public class Flink04_Practice_UserVisitor_Window2 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定eventtime时间字段
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getTimestamp() * 1000L);

        //读取数据并转换成JavaBean
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                }).filter(data -> "pv".equals(data.getBehavior())).assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);
        //按key分组并开窗
        WindowedStream<UserBehavior, String, TimeWindow> window = userBehaviorDS.keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.hours(1)));
        //进行计算并输出
        window.process(new ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow>() {
            private MapState<Long,Integer> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Integer>("value", Long.class,Integer.class));
            }

            @Override
            public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
                Iterator<UserBehavior> iterator = elements.iterator();

                while (iterator.hasNext()) {
                    mapState.put(iterator.next().getUserId(),1);
                }


                Integer count = (int)mapState.keys().spliterator().estimateSize();

                long end = context.window().getEnd();

                Timestamp ts = new Timestamp(end);
                out.collect(new UserVisitorCount("uv",ts.toString(),count));

                mapState.clear();
            }
        }).print();
        //执行环境
        env.execute();
    }
}
