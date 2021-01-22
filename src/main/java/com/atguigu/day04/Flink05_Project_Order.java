package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;

/**
 * @author Aol
 * @create 2021-01-22 13:40
 */
public class Flink05_Project_Order {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> orderEvent = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> txEvent = env.readTextFile("input/ReceiptLog.csv");

        SingleOutputStreamOperator<OrderEvent> orDS = orderEvent.flatMap((String line, Collector<OrderEvent> word) -> {
            String[] split = line.split(",");
            if (split[2] != null) {
                OrderEvent oe = new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                word.collect(oe);
            }
        }).returns(OrderEvent.class);

        SingleOutputStreamOperator<TxEvent> teDS = txEvent.map(s -> {
            String[] split = s.split(",");
            return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
        });

        KeyedStream<OrderEvent, String> orderEventStringKeyedStream = orDS.keyBy(OrderEvent::getTxId);

        KeyedStream<TxEvent, String> txEventStringKeyedStream = teDS.keyBy(TxEvent::getTxId);

        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventStringKeyedStream.connect(txEventStringKeyedStream);

        connect.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>() {

            HashMap<String,OrderEvent> orderEventHashSet = new HashMap<>();
            HashMap<String,TxEvent> txEventHashSet = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

                if (txEventHashSet.containsKey(value.getTxId())){
                    out.collect(Tuple2.of(value,txEventHashSet.get(value.getTxId())));
                }else {
                    orderEventHashSet.put(value.getTxId(),value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

                if (orderEventHashSet.containsKey(value.getTxId())){
                    out.collect(Tuple2.of(orderEventHashSet.get(value.getTxId()),value));
                }else {
                    txEventHashSet.put(value.getTxId(),value);
                }
            }
        }).print();

        env.execute();
    }
}
