package com.atguigu.day08;

import com.atguigu.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Aol
 * @create 2021-01-30 15:25
 */
public class Flink04_Practice_LoginApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定时间字段
        WatermarkStrategy<LoginEvent> loginEventWatermarkStrategy = WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((e, s) -> e.getEventTime() * 1000L);

        //转化数据结果为JavaBean
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(loginEventWatermarkStrategy);

        //按userid分组运算
        SingleOutputStreamOperator<String> process = loginEventDS.keyBy(LoginEvent::getUserId)
                .process(new LogKeyFunc(2,2));

        //打印数据
        process.print();

        //执行环境
        env.execute();
    }

    private static class LogKeyFunc extends KeyedProcessFunction<Long,LoginEvent,String> {

        private Integer count;
        private Integer timemax;

        private ListState<LoginEvent> loginEventListState;

        private ValueState<Long> longValueState;

        public LogKeyFunc(Integer count, Integer timemax) {
            this.count = count;
            this.timemax = timemax;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list", LoginEvent.class));
            longValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

            String eventType = value.getEventType();
            Iterable<LoginEvent> loginEvents = loginEventListState.get();
            Long aLong = longValueState.value();


            if ("fail".equals(eventType)) {
                loginEventListState.add(value);

                if (!loginEvents.iterator().hasNext()) {

                    long l = ctx.timerService().currentWatermark() + timemax * 1000L;
                    ctx.timerService().registerEventTimeTimer(l);
                    longValueState.update(l);
                }
            } else {

                if (aLong != null) {
                    ctx.timerService().deleteEventTimeTimer(aLong);
                }
                longValueState.clear();
                loginEventListState.clear();
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {


            Iterator<LoginEvent> iterator = loginEventListState.get().iterator();

            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(iterator);

            if (loginEvents.size() >= count){

                LoginEvent first = loginEvents.get(0);
                LoginEvent last = loginEvents.get(loginEvents.size() - 1);
                out.collect(first.getUserId() +
                        "用户在" +
                        first.getEventTime() +
                        "到" +
                        last.getEventTime() +
                        "之间，连续登录失败了" +
                        loginEvents.size() + "次！");
            }

            longValueState.clear();
            loginEventListState.clear();
        }
    }
}
