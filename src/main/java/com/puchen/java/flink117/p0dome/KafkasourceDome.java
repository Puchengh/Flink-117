package com.puchen.java.flink117.p0dome;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkasourceDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //从kafka中读取数据 新的source架构
            //Kafka消费
                //earliest ： 如果有offset  从offset开始消费 如果没有offset就从最早开始消费
                //laster: 如果有offset 从offset开始消费  如果没有offset就从最新的输入开始消费
            //flink的KafkaSource消费:OffsetsInitializer 默认是earliest
                //earliest 一定是从最早开始消费
                //latest  一定是从最新开始消费
        KafkaSource<String> build = KafkaSource.<String>builder()
                .setBootstrapServers("master:9092,master:9093,master:9094")   //指定kafka的地址和端口
                .setGroupId("puchen")//指定消费组的id
                .setTopics("itcast")    //指定消费者的topic
                .setValueOnlyDeserializer(new SimpleStringSchema())  //指定 反序列化器 这个是烦序列化的value
                .setStartingOffsets(OffsetsInitializer.latest())  //flink消费kafka的特点
                .build();

        env.fromSource(build,WatermarkStrategy.noWatermarks(),"source_file").print();

        env.execute();
    }
}
