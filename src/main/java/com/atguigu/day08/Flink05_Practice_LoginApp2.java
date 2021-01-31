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
 * @create 2021-01-30 16:00
 */
public class Flink05_Practice_LoginApp2 {
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
                .process(new LogKeyFunc(2));

        //打印数据
        process.print();

        //执行环境
        env.execute();
    }

    private static class LogKeyFunc extends KeyedProcessFunction<Long,LoginEvent,String> {

        private Integer ts;



        private ValueState<LoginEvent> longValueState;

        public LogKeyFunc(Integer ts) {
            this.ts = ts;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            longValueState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("value", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {


            if ("fail".equals(value.getEventType())) {


                LoginEvent value1 = longValueState.value();

                longValueState.update(value);

                if (value1 != null && Math.abs(value.getEventTime() - value1.getEventTime()) <= ts) {

                    //输出报警信息
                    out.collect(value.getUserId() + "连续登录失败2次！");
                }
            } else {

                longValueState.clear();

            }

        }


    }
}
