package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author Aol
 * @create 2021-01-24 16:31
 */
public class Flink11_State_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            private ValueState<String> valueState;
            private ListState<String> listState;
            private ReducingState<WaterSensor> reducingState;
            private AggregatingState<WaterSensor, WaterSensor> aggregatingState;
            private MapState<String, Integer> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value", String.class));
                listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list", String.class));
//                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>());
//                aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, WaterSensor, WaterSensor>());
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("map", String.class, Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                valueState.update(value.getId());
                valueState.value();
                valueState.clear();

                listState.addAll(new ArrayList<>());
                listState.update(new ArrayList<>());
                listState.add(value.getId());
                listState.clear();
                listState.get();

                reducingState.add(value);
                reducingState.clear();
                reducingState.get();

                aggregatingState.add(value);
                aggregatingState.clear();
                aggregatingState.clear();

                mapState.contains(value.getId());
                mapState.entries();
                mapState.get(value.getId());
                mapState.iterator();
                mapState.clear();
                mapState.isEmpty();
//                mapState.put();
            }
        });

        process.print();

        env.execute();
    }
}
