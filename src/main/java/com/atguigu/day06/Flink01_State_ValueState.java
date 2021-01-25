package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-25 11:19
 */
public class Flink01_State_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        keyedStream.flatMap(new RichFlatMapFunction<WaterSensor, String>() {
            private ValueState<Integer> lastVc;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value_state", Integer.class));
            }

            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {


                Integer vc_v = lastVc.value();


                if (vc_v != null && Math.abs(vc_v - value.getVc()) > 10) {
                    out.collect(value.getId() + "温度差值超过10度!!");
                }

                lastVc.update(value.getVc());
            }
        }).print();

        env.execute();
    }
}
