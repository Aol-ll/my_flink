package com.atguigu.day06;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Aol
 * @create 2021-01-26 11:30
 */
public class Flnk09_State_WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置执行后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink2021/ck"));

        //开启ck
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //读取端口数据并转为元组
        env.socketTextStream("hadoop102", 9999)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] split = line.split(" ");
                    for (String s : split) {
                        out.collect(Tuple2.of(s, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(key -> key.f0)
                .sum(1)
                .print();
        //执行任务
        env.execute();
    }
}
