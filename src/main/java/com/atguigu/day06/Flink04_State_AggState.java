package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-25 11:56
 */
public class Flink04_State_AggState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            private AggregatingState<WaterSensor, Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggregatingState = getRuntimeContext()
                        .getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Tuple2<Integer, Integer>,
                                Double>("agg_state", new AggregateFunction<WaterSensor, Tuple2<Integer, Integer>, Double>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0,0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(WaterSensor value, Tuple2<Integer, Integer> accumulator) {
                                return Tuple2.of(value.getVc() + accumulator.f0,1 + accumulator.f1);
                            }

                            @Override
                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                return accumulator.f0.doubleValue() / accumulator.f1;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                return Tuple2.of(a.f0 + b.f0,a.f1+b.f1);
                            }
                        }, Types.TUPLE(Types.INT,Types.INT)));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                aggregatingState.add(value);

                Double aDouble = aggregatingState.get();

                out.collect(value.getId() + "的平均水位"+ aDouble);
            }
        }).print();

        env.execute();
    }
}
