package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Aol
 * @create 2021-01-25 13:29
 */
public class Flink06_State_ListState_OP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        dataStreamSource.map(new MyCountMapper()).print();

        env.execute();

    }

    private static class MyCountMapper implements MapFunction<String,Long>, CheckpointedFunction {

        private ListState<Long> longListState;
        private Long count = 0L;

        @Override
        public Long map(String value) throws Exception {

            return Long.parseLong(value.split(",")[1]);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            longListState.clear();
            longListState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            longListState = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("list_state", Long.class));

            for (Long aLong : longListState.get()) {
                count += aLong;
            }
        }
    }
}
