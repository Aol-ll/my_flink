package com.atguigu.day09;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-02-01 11:44
 */
public class Flink02_Practice_OrderReceiptWithJoin {
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

        orDS.keyBy(OrderEvent::getTxId)
                .intervalJoin(teDS.keyBy(TxEvent::getTxId))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>() {
                    @Override
                    public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                        out.collect(Tuple2.of(left,right));
                    }
                })
                .print();

        env.execute();
    }
}
