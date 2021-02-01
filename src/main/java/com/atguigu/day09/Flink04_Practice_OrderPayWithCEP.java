package com.atguigu.day09;

import com.atguigu.bean.LoginEvent;
import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author Aol
 * @create 2021-01-30 18:45
 */
public class Flink04_Practice_OrderPayWithCEP {
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
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //定义模式序列
        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern.<OrderEvent>begin("start").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                return "create".equals(orderEvent.getEventType());
            }
        }).next("next").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                return "pay".equals(orderEvent.getEventType());
            }
        }).within(Time.minutes(15));

        SingleOutputStreamOperator<String> select = CEP.pattern(keyedStream, orderEventPattern)
                .select(
                        new OutputTag<String>("no pay") {
                        },
                        new PatternTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                                return map.get("start").get(0).getOrderId() + "超出支付时间!!";
                            }
                        },
                        new PatternSelectFunction<OrderEvent, String>() {
                            @Override
                            public String select(Map<String, List<OrderEvent>> map) throws Exception {


                                OrderEvent start = map.get("start").get(0);
                                OrderEvent end = map.get("next").get(0);

                                return start.getOrderId() + "在 " + start.getEventTime()
                                        + " 到 " + end.getEventTime() + " 支付成功";
                            }
                        }
                );
        //输出结果
        select.print();
        select.getSideOutput(new OutputTag<String>("no pay") {
        }).print();
        //执行环境
        env.execute();

    }


}
