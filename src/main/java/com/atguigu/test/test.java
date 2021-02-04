package com.atguigu.test;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Aol
 * @create 2021-02-03 19:08
 */
public class test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.10
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((e, s) -> e.getTimestamp() * 1000L);

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        Table table = tableEnv.fromDataStream(userBehaviorDS,
                $("userId"),
                $("itemId"),
                $("categoryId"),
                $("behavior"),
                $("timestamp"),
                $("rt").rowtime());



        Table result = tableEnv.sqlQuery("select " +
                "itemId," +
                "count(itemId) as ct," +
                "HOP_end(rt, INTERVAL '5' minute,INTERVAL '1' hour) as windowEnd from " +
                table +
                " group by itemId,HOP(rt, INTERVAL '5' minute,INTERVAL '1' hour)");

        Table query = tableEnv.sqlQuery("select " +
                "itemId," +
                "ct," +
                "windowEnd," +
                "row_number()over(partition by windowEnd order by ct desc) as rn from " +
                result);

        Table table1 = tableEnv.sqlQuery("select * from " + query + " where rn<=5");

/*              Table result = tableEnv.sqlQuery("select " +
                "id," +
                "count(id)over w," +
                "sum(vc)over w from " +
                table  +
                " window w as (partition by id order by pt rows between 2 preceding and current row)");*/

        table1.execute().print();

        env.execute();

    }
}
