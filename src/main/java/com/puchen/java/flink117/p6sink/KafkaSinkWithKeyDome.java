package com.puchen.java.flink117.p6sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @ClassName: KafkaSinkDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 17:19
 * @Version: 1.0
 **/
public class KafkaSinkWithKeyDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 如果要指定写入kafka的key
         * 可以自定义反序列化器
         * 1、实现一个接口 重写系列化方法
         * 2、指定key 转化成字节数组
         * 3、 指定key 转成字节数组
         * 4、返回一个 PriducerRecord对象 把key value放进去
         */
        SingleOutputStreamOperator<String> master = env.socketTextStream("master", 7777);
        env.setParallelism(1);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);  //精准一次需要开启checkpoint 否则无法写入kafka

        KafkaSink<String> ws = KafkaSink.<String>builder()
                .setBootstrapServers("master:9092,slave1:9092,slave2:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                String[] datas = element.split(",");
                                byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);

                                return new ProducerRecord("ws", key, value);
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("puchen-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();

        master.sinkTo(ws);
        env.execute();

    }

}
