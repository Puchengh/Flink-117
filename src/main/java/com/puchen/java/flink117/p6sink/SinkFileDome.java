package com.puchen.java.flink117.p6sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @ClassName: SinkFileDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 16:27
 * @Version: 1.0
 **/
public class SinkFileDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        //必须要开启checkpoint 否则一直都是.inprogress 文件不能被读取
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        //从kafak中读取数据
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "Number" + aLong;
            }
        },
                Long.MIN_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );

        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        //输出到文件系统
        //输出行行式存储的文件 指定路劲 指定编码
        @SuppressWarnings("unchecked") FileSink<String> build = FileSink
                .<String>forRowFormat(new Path("E:\\study\\Flink-117\\input"), new SimpleStringEncoder("UTF-8"))
                .withOutputFileConfig(OutputFileConfig
                        .builder()
                        .withPartPrefix("puchen-")
                        .withPartSuffix(".log")
                        .build()
                )
                //分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneId.systemDefault()))
                //文件滚动策略  10s  或者 1M
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                )
                .build();
        dataGen.sinkTo(build);

        env.execute();
    }

}
