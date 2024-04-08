package com.puchen.java.flink117.p0dome;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {
    /**
     * 数据生成器的source 四个参数：
     * 1.GeneratorFunction接口  需要实现 重写Map方法  输入类型固定是Long
     * 2.Long类型 自动生成数字序列（从1开始自增）的最大值 达到这个值就停止了
     * 3.限速策略 比如：美妙生成几条数据
     * 4.返回的类型
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataGeneratorSource<String> build = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {
                        return "Number:" + aLong;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );
        env.fromSource(build, WatermarkStrategy.noWatermarks(), "source_file").print();
        env.execute();
    }
}
