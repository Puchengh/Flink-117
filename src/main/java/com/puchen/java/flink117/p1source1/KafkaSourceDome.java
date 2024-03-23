package com.puchen.java.flink117.p1source1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: KafkaSource
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 8:42
 * @Version: 1.0
 **/
public class KafkaSourceDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafak中读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("master:9092,slave1:9092,slave2:9092")
                .setGroupId("puchen")  //指定消费者的组
                .setTopics("itcast")  //指定消费者的topic
                .setValueOnlyDeserializer(new SimpleStringSchema())  //只对walue反序列化  setDeserializer 对key和value都序列化
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkasource").print();
        env.execute();
    }

}
