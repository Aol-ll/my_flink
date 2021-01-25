package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-25 11:42
 */
public class Flink03_State_ReducingState {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        //数据转换成JavaBean
        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        //按WaterSensor中的vc分组
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        //计算每个传感器的水位和
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private ReducingState<WaterSensor> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState =
                        getRuntimeContext()
                                .getReducingState(new ReducingStateDescriptor<WaterSensor>("reduce_state", new ReduceFunction<WaterSensor>() {
                                    @Override
                                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                        return new WaterSensor(value1.getId(), Math.max(value1.getTs(), value2.getTs()), value1.getVc() + value2.getVc());
                                    }
                                }, WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                reducingState.add(value);

                WaterSensor waterSensor = reducingState.get();

                out.collect(waterSensor);

            }
        }).print();

        //执行任务
        env.execute();
    }
}
