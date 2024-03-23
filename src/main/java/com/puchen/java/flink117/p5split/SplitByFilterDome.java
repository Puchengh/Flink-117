package com.puchen.java.flink117.p5split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: SplitByFilterDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 11:09
 * @Version: 1.0
 **/
public class SplitByFilterDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<String> master = env.socketTextStream("master", 7777);

        /**
         * 实现了分流的效果
         * 缺点:同一个数据需要被处理两次  性能效果很差
         */
//        master.filter(value -> Integer.parseInt(value) % 2 == 0).print("偶数流");
//
//        master.filter(value -> Integer.parseInt(value) % 2 == 1).print("基数流");
        /**
         * 侧输出流
         */

        env.execute();
    }

}
