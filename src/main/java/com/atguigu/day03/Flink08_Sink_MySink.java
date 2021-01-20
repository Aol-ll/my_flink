package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author Aol
 * @create 2021-01-20 18:04
 */
public class Flink08_Sink_MySink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        });

        map.addSink(new MySink());

        env.execute();

    }

    private static class MySink extends RichSinkFunction<WaterSensor> {
        private PreparedStatement preparedStatement;

        private Connection connection;
        @Override
        public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false","root","123456");

                preparedStatement= connection.prepareStatement("INSERT INTO `sensor` VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?");



        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            preparedStatement.setString(1,value.getId());
            preparedStatement.setLong(2,value.getTs());
            preparedStatement.setInt(3,value.getVc());
            preparedStatement.setLong(4,value.getTs());
            preparedStatement.setInt(5,value.getVc());
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {


            preparedStatement.close();
            connection.close();
        }
    }
}
