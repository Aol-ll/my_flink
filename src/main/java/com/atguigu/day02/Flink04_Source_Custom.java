package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @author Aol
 * @create 2021-01-19 17:59
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.addSource(new MySource("hadoop102", 9999));

        source.print();

        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private String host;
        private int port;

        private boolean flag = true;

        Socket sc = null;
        BufferedReader bf = null;


        public MySource() throws IOException {
        }

        public MySource(String host, int port) throws IOException {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            sc = new Socket(host, port);
            bf = new BufferedReader(new InputStreamReader(sc.getInputStream(), StandardCharsets.UTF_8));

            String line = bf.readLine();
            while (flag && line != null) {

                String[] split = line.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                ctx.collect(waterSensor);
                line = bf.readLine();

            }
        }

        @Override
        public void cancel() {
            flag = false;
            try {
                bf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                sc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
