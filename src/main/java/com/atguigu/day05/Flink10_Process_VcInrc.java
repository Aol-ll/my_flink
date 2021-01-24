package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Aol
 * @create 2021-01-24 15:20
 */
public class Flink10_Process_VcInrc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(data -> {
            String[] split = data.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<WaterSensor> sidout = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            Integer lastVc = Integer.MIN_VALUE;
            long ts = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                Integer vc = value.getVc();

                if (vc >= lastVc && ts == Long.MIN_VALUE) {
                    ts = ctx.timerService().currentProcessingTime() + 10000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);

                } else if (vc < lastVc) {

                    ctx.timerService().deleteProcessingTimeTimer(ts);
                    ts = Long.MIN_VALUE;
                }
                lastVc = vc;

                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("sidout"){},
                        ctx.getCurrentKey() + "连续10s水位线没有下降!");
                ts = Long.MIN_VALUE;
            }
        });

        sidout.print("main");
        sidout.getSideOutput(new OutputTag<String>("sidout") {
        }).print("sid");


        env.execute();

    }
}
