package com.puchen.java.flink117.p8water_marks;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WaterMarksDome1
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/24 19:37
 * @Version: 1.0
 **/
public class WaterMarksOrderDome1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777)
                .map(new WaterSensorMapFunction());

        //指定wtareMarks
        //1.升序watermark策略
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                //升序的watermark 没有等待时间
                .<WaterSensor>forMonotonousTimestamps()
                //指定 时间戳分配器  从数据中提取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        //返回的时间戳  毫秒
                        System.out.println("数据=" + waterSensor + ",recordTSA" + l);
                        return waterSensor.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        KeyedStream<WaterSensor, String> sensorKS = waterSensorSingleOutputStreamOperator.keyBy(r -> r.getId());
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
