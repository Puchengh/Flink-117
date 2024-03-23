package com.puchen.java.flink117.p5split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: UnionDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 14:56
 * @Version: 1.0
 **/
public class UnionDome {

    public static void main(String[] args) throws Exception {
        /**
         * 1.流的数据类型必须一致
         * 2.一次可以合并多个流
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33, 44, 55);
        DataStreamSource<String> source3 = env.fromElements("111", "222", "333");

        DataStream<Integer> union1 = source1.union(source2).union(source3.map(r -> Integer.valueOf(r)));
        DataStream<Integer> union2 = source1.union(source2, source3.map(r -> Integer.valueOf(r)));
        union2.print();
//        union1.print();
        env.execute();

    }

}
