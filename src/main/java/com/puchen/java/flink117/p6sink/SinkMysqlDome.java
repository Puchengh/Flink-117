package com.puchen.java.flink117.p6sink;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @ClassName: SinkMysqlDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 18:04
 * @Version: 1.0
 **/
public class SinkMysqlDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        /**
         * 1、只能用来的sink写法  addsink
         * 2.JDBCSink的四个参数
         *  第一参数 执行的sql 一般都是insert into
         *  第二个参数 预编译sql 对占位符填充值
         *  第三个参数 执行选项---》攒批 充实
         *  第四个参数 链接选项 ---》url 用户
         */
        SingleOutputStreamOperator<WaterSensor> master = env.socketTextStream("master", 7777).map(new WaterSensorMapFunction());
        SinkFunction<WaterSensor> jdbcTest = JdbcSink.sink("insert into ws value(?,?,?)",
                (JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());

                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.116.135:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull")
                        .withUsername("test")
                        .withPassword("123456")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );

        master.addSink(jdbcTest);
        env.execute();
    }

}
