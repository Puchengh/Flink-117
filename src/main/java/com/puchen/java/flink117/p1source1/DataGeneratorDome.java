package com.puchen.java.flink117.p1source1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: DataGeneratorDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 9:02
 * @Version: 1.0
 **/
public class DataGeneratorDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从kafak中读取数据
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "Number" + aLong;
            }
        },
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "kafkasource").print();
        env.execute();
    }

}
