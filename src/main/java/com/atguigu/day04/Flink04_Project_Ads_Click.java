package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Aol
 * @create 2021-01-22 13:34
 */
public class Flink04_Project_Ads_Click {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        streamSource.map(s->{
            String[] split = s.split(",");

            return new AdsClickLog(Long.parseLong(split[0]),Long.parseLong(split[1]),split[2],split[3],Long.parseLong(split[4]));
        }).map(s-> Tuple2.of(s.getProvince() + "-" +s.getAdId(),1))
                .returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(key->key.f0)
                .sum(1).print();

        env.execute();


    }
}
