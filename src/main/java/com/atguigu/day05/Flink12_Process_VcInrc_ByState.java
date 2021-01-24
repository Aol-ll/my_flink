package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Aol
 * @create 2021-01-24 17:01
 */
public class Flink12_Process_VcInrc_ByState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private ValueState<Integer> lastVc;

            private ValueState<Long> ts;
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastvc_state", Integer.class));

                ts = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts_state", Long.class));
            }


            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                Integer vc = value.getVc();

                Integer lastVc_v = lastVc.value();
                Long ts_v = ts.value();

                if (lastVc_v == null || (vc >= lastVc_v && ts_v == null)){
                    long time = ctx.timerService().currentProcessingTime() + 10000L;

                    ctx.timerService().registerProcessingTimeTimer(time);

                    ts.update(time);
                }else if (vc < lastVc_v && ts_v != null){
                    ctx.timerService().deleteProcessingTimeTimer(ts_v);

                    ts.clear();
                }
                lastVc.update(vc);

                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                ctx.output(new OutputTag<String>("sid") {
                },ctx.getCurrentKey() + "连续10s水位线没有下降!");

                ts.clear();
            }
        });

        process.print("main");
        process.getSideOutput(new OutputTag<String>("sid") {
        }).print("side");

        env.execute();
    }
}
