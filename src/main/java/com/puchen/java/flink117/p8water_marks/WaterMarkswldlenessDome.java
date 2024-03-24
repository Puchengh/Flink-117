package com.puchen.java.flink117.p8water_marks;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import com.puchen.java.flink117.p4physical_partitionsing.MyPartitioner;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName: WaterMarksDome1
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/24 19:37
 * @Version: 1.0
 **/
public class WaterMarkswldlenessDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        /**
         * 自定义分区器： 数据%分区数据 只输入奇数 都只会往map的一个子任务
         */
        SingleOutputStreamOperator<Integer> sensorDS = env.socketTextStream("slave2", 7777)
                .partitionCustom(new MyPartitioner(), r -> r)
                .map(r -> Integer.parseInt(r))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Integer>forMonotonousTimestamps()
                        .withTimestampAssigner((r, ts) -> r * 1000L)
                        .withIdleness(Duration.ofSeconds(5))   //空闲等待五秒
                );
        /**
         * 分成两组 奇数一组 偶数一组 开10s的事件事件滚动窗口
         */
        sensorDS
                .keyBy(r-> r % 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowStrat = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowend = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + integer + "的窗口[" + windowStrat + "," + windowend + "]包含" + count + "条数据====>" + elements.toString());

                    }
                }).print();
        env.execute();
    }
}
