package com.puchen.java.flink117.p6sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @ClassName: KafkaSinkDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 17:19
 * @Version: 1.0
 **/
public class KafkaSinkDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * kafka sink
         * 注意：如果要使用精准一次 写入kafka  需要满足一下条件 缺一不可
         * 1.开始checkpoint
         * 2.设置事务前缀
         * 3.设置事务超时时间 checkpoint间隔 < 事务超时时间 < max的15分钟
         */
        SingleOutputStreamOperator<String> master = env.socketTextStream("master", 7777);
        env.setParallelism(1);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);  //精准一次需要开启checkpoint 否则无法写入kafka

        KafkaSink<String> ws = KafkaSink.<String>builder()
                //指定kafka的地址和端口
                .setBootstrapServers("master:9092,slave1:9092,slave2:9092")
                //指定序列化器 指定topic名称 具体的序列化
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("ws")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //写到kafka的一致性  精准一次 至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("puchen-")
                //如果是精准一次 必须设置事务超时时间  checkpoint间隔 < 事务超时时间 < max的15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"")
                .build();

        master.sinkTo(ws);
        env.execute();

    }

}
