package com.puchen.java.flink117.p8water_marks;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
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
public class WaterMarksCustomDome3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //默认周期是200ms
        env.getConfig().setAutoWatermarkInterval(2000);

        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777)
                .map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                // 自定义的生成器
//                .<WaterSensor>forGenerator(
//                        ctx -> new MyGenerator<>(3000)  //3s
//                )
                .<WaterSensor>forGenerator(
                        ctx -> new MyPunctuatedGenerator<>(3000)  //3s
                )
                .withTimestampAssigner((waterSensor, l) -> {
                            //返回的时间戳  毫秒
                            System.out.println("数据=" + waterSensor + ",recordTSA" + l);
                            return waterSensor.getTs() * 1000L;
                        }
                );
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy).keyBy(r -> r.getId());
        //需要指定 时间语义TumblingEventTimeWindows的窗口
        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> process = sersonWS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowStrat = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowend = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口[" + windowStrat + "," + windowend + "]包含" + count + "条数据====>" + elements.toString());

                    }
                }
        );
        process.print();
        env.execute();
    }
}
