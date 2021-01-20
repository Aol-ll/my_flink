package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Aol
 * @create 2021-01-20 18:25
 */
public class Flink09_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        map.addSink(JdbcSink.sink(
                "INSERT INTO `sensor` VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?",
                (pe,l)->{
                    pe.setString(1,l.getId());
                    pe.setLong(2,l.getTs());
                    pe.setInt(3,l.getVc());
                    pe.setLong(4,l.getTs());
                    pe.setInt(5,l.getVc());
                    },
                new JdbcExecutionOptions.Builder()
                .withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
                ));

        env.execute();
    }
}