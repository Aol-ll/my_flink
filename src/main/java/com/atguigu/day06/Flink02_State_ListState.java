package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


/**
 * @author Aol
 * @create 2021-01-25 11:31
 */
public class Flink02_State_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        keyedStream.map(new RichMapFunction<WaterSensor, List<WaterSensor>>() {
            private ListState<WaterSensor> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<WaterSensor>("list_state", WaterSensor.class));
            }

            @Override
            public List<WaterSensor> map(WaterSensor value) throws Exception {

                listState.add(value);

                ArrayList<WaterSensor> arrayList = Lists.newArrayList(listState.get().iterator());

                arrayList.sort((l, r) -> r.getVc() - l.getVc());

                if (arrayList.size() > 3){
                    arrayList.remove(3);
                }

                listState.update(arrayList);

                return arrayList;
            }
        }).print();

        env.execute();
    }
}
