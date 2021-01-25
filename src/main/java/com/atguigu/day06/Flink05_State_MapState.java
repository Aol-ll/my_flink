package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author Aol
 * @create 2021-01-25 13:15
 */
public class Flink05_State_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private MapState<Integer, String> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, String>("map_state", Integer.class, String.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {


                if (!mapState.contains(value.getVc())){

                    out.collect(value);
                    mapState.put(value.getVc(),"00");

                }
            }
        }).print();

        env.execute();
    }
}
