package com.atguigu.day09;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Aol
 * @create 2021-02-01 10:06
 */
public class Flink01_Practice_OrderReceiptWithState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> orderEvent = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> txEvent = env.readTextFile("input/ReceiptLog.csv");

        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getEventTime() * 1000L);
        WatermarkStrategy<TxEvent> txEventWatermarkStrategy = WatermarkStrategy.<TxEvent>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getEventTime() * 1000L);

        SingleOutputStreamOperator<OrderEvent> orDS = orderEvent.flatMap((String line, Collector<OrderEvent> word) -> {
            String[] split = line.split(",");
            if ("pay".equals(split[1])) {
                OrderEvent oe = new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                word.collect(oe);
            }
        }).returns(OrderEvent.class).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        SingleOutputStreamOperator<TxEvent> teDS = txEvent.map(s -> {
            String[] split = s.split(",");
            return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
        }).assignTimestampsAndWatermarks(txEventWatermarkStrategy);

        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> process = orDS.connect(teDS).keyBy("txId", "txId")
                .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>() {

                    private ValueState<OrderEvent> orderState;
                    private ValueState<TxEvent> txState;

                    private ValueState<Long> timerState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("or", OrderEvent.class));
                        txState = getRuntimeContext().getState(new ValueStateDescriptor<TxEvent>("tx", TxEvent.class));

                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
                    }

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

                        if (txState.value() == null) {
                            orderState.update(value);
                            long ts = (value.getEventTime() + 10) * 1000L;
                            ctx.timerService().registerEventTimeTimer(ts);
                            timerState.update(ts);
                        } else {
                            out.collect(Tuple2.of(value,txState.value()));
                            ctx.timerService().deleteEventTimeTimer(timerState.value());
                            txState.clear();
                            timerState.clear();

                        }

                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

                        if (orderState.value() == null) {

                            txState.update(value);
                            long ts = (value.getEventTime() + 5) * 1000L;
                            ctx.timerService().registerEventTimeTimer(ts);
                            timerState.update(ts);
                        } else {

                            out.collect(Tuple2.of(orderState.value(),value));
                            ctx.timerService().deleteEventTimeTimer(timerState.value());
                            timerState.clear();
                            orderState.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

                        OrderEvent orderEvent1 = orderState.value();
                        TxEvent txEvent1 = txState.value();

                        if (orderEvent1 == null) {
                            ctx.output(new OutputTag<String>("no pay"){},txEvent1.getTxId() +"没有支付信息");
                        } else {
                            ctx.output(new OutputTag<String>("no tx"){},orderEvent1.getTxId() +"没有到账信息");
                        }


                        orderState.clear();
                        txState.clear();
                        timerState.clear();
                    }
                });

        process.print();
        process.getSideOutput(new OutputTag<String>("no pay"){}).print("pay");
        process.getSideOutput(new OutputTag<String>("no tx"){}).print("tx");

        env.execute();

    }
}
