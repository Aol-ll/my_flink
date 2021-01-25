package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-25 13:42
 */
public class Flink07_State_BroadState_OP {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //2.读取流中的数据
        DataStreamSource<String> propertiesStream = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<String, String>("map_state", String.class, String.class);
        BroadcastStream<String> broadcast = propertiesStream.broadcast(descriptor);

        BroadcastConnectedStream<String, String> connect = dataStream.connect(broadcast);

        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);

                String aSwitch = broadcastState.get("Switch");

                if ("1".equals(aSwitch)){
                    out.collect("读取了广播状态,切换1");
                } else if ("2".equals(aSwitch)) {
                    out.collect("读取了广播状态,切换2");
                } else {
                    out.collect("读取了广播状态,切换其他");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);

                broadcastState.put("Switch",value);
            }
        }).print();

        env.execute();

    }
}
