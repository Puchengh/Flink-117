package com.puchen.java.flink117.p5split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @ClassName: ConnectDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 15:02
 * @Version: 1.0
 **/
public class ConnectDome {

    public static void main(String[] args) throws Exception {
        /**
         *
         * keyby 之后得到keyedstream
         * 算子计算之后可以得到DataStream
         *
         * 1.一次只能链接两条流
         * 2.流的数据类型呢够可以不一样
         * 3.连接后可以调用map flatmap process来处理 但是各处理各的
         *  CoProcessFunction 关联多流
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(1);
//        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
//        DataStreamSource<String> source3 = env.fromElements("a", "b", "c");
        SingleOutputStreamOperator<Integer> source1 = env.socketTextStream("master", 7777).map(i -> Integer.parseInt(i));
        DataStreamSource<String> source3 = env.socketTextStream("master", 8888);

        ConnectedStreams<Integer, String> connect = source1.connect(source3);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流:"+value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字母流:"+value;
            }
        });

        map.print();

        env.execute();

    }


}
