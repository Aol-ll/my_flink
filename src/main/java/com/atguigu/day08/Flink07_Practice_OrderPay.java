package com.atguigu.day08;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Aol
 * @create 2021-01-30 18:45
 */
public class Flink07_Practice_OrderPay {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //指定watermark
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getEventTime() * 1000L);

        //转化数据结构和JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        //按OrderId分组计算
        SingleOutputStreamOperator<String> process = orderEventDS.keyBy(OrderEvent::getOrderId)
                .process(new myProcessFunc(15));
        //输出结果
        process.print("main");
        process.getSideOutput(new OutputTag<String>("sid1"){}).print("sid1");
        process.getSideOutput(new OutputTag<String>("sid2"){}).print("sid2");
        //执行环境
        env.execute();

    }

    private static class myProcessFunc extends KeyedProcessFunction<Long, OrderEvent, String> {

        private ValueState<OrderEvent> valueState;
        private ValueState<Long> ts;

        private Integer interval;

        public myProcessFunc(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("va", OrderEvent.class));
            ts = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            if ("create".equals(value.getEventType())) {

                valueState.update(value);
                Long eventTime = (value.getEventTime() + interval * 60) * 1000L;
                ctx.timerService().registerEventTimeTimer(eventTime);
                ts.update(eventTime);
            } else if ("pay".equals(value.getEventType())) {
                if (valueState.value() == null) {

                    ctx.output(new OutputTag<String>("sid1"){},
                            ctx.getCurrentKey() + "没创建");

                } else {

                    out.collect(ctx.getCurrentKey() + "正常输出");
                    ctx.timerService().deleteEventTimeTimer(ts.value());
                    ts.clear();
                    valueState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            ctx.output(new OutputTag<String>("sid2"){},
                    valueState.value().getOrderId() + "没支付");

            ts.clear();
            valueState.clear();

        }
    }
}
