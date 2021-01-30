package com.atguigu.day07;


import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;

/**
 * @author Aol
 * @create 2021-01-29 17:08
 */
public class Flink05_Practice_UserVisitor_Window3 {
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

        //组内聚合
        window.trigger(new Trigger<UserBehavior, TimeWindow>() {
            @Override
            public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.FIRE_AND_PURGE;
            }

            @Override
            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

            }
        }).process(new ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow>() {

            private Myboolm myboolm;

            private Jedis jedis;

            private String hashKey;

            @Override
            public void open(Configuration parameters) throws Exception {
                myboolm = new Myboolm(1 << 30);

                jedis = new Jedis("hadoop102", 6379);

                hashKey = "HourUv";
            }

            @Override
            public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {

                UserBehavior next = elements.iterator().next();


                String windowTime = new Timestamp(context.window().getEnd()).toString();

                String ts = "BitMap_"+windowTime;

                long getoffest = myboolm.getoffest(next.getUserId().toString());


                Boolean getbit = jedis.getbit(ts, getoffest);

                if (!getbit) {

                    jedis.hincrBy(hashKey, windowTime, 1);

                    jedis.setbit(ts, getoffest, true);
                }

                String hget = jedis.hget(hashKey, windowTime);

                out.collect(new UserVisitorCount("uv",windowTime,Integer.parseInt(hget)));


            }
        }).print();

        env.execute();

    }

    //自定义boolm过滤器
    private static class Myboolm {
        private long cap;

        public Myboolm(long cap) {
            this.cap = cap;
        }

        public long getoffest(String value) {
            long offest = 0L;
            char[] chars = value.toCharArray();
            for (char c : chars) {
                offest = offest * 31 + c;
            }
            return offest & (cap - 1);
        }
    }
}
