package com.puchen.java.flink117.p4physical_partitionsing;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: PartitionCustomDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 11:00
 * @Version: 1.0
 **/
public class PartitionCustomDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<String> master = env.socketTextStream("master", 7777);
//        master.partitionCustom(new MyPartitioner(),r -> r).print();
        master.partitionCustom(new MyPartitioner(), new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }


}
