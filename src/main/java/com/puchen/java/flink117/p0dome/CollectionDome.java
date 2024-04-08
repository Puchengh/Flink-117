package com.puchen.java.flink117.p0dome;

import java.util.Arrays;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CollectionDome {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从集合来读取数据
        DataStreamSource<Integer> source = env
//                    .fromCollection(Arrays.asList(1, 22, 3));
                .fromElements(1, 22, 3);
        source.print();
        env.execute();
    }
}
