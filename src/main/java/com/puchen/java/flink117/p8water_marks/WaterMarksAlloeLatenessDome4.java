package com.puchen.java.flink117.p8water_marks;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @ClassName: WaterMarksAlloeLatenessDome4
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/24 19:37
 * @Version: 1.0
 **/
public class WaterMarksAlloeLatenessDome4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        /**
         * 乱序: 数据的顺序乱了 出现时间小的比时间打的晚来
         * 迟到: 当前数据的时间戳 < 当前的watermark
         * 窗口允许迟到
         * 1.推迟关窗时间 在关窗之前 迟到数据来了 还能被窗口计算 来条迟到数据触发计算一次
         * 2.关窗后 迟到的数据不会被计算
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777)
                .map(new WaterSensorMapFunction());

        //指定wtareMarks
        //1.升序watermark策略
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((waterSensor,l) -> {
                    System.out.println("数据=" + waterSensor + ",recordTSA" + l);
                    return waterSensor.getTs() * 1000L;
                }
        );
        OutputTag<WaterSensor> waterSensorOutputTag = new OutputTag<>("later-date", Types.POJO(WaterSensor.class));
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy).keyBy(r -> r.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2)) //推迟两秒关窗
                .sideOutputLateData(waterSensorOutputTag)   //关窗后的迟到的数据放入侧输出流
                ;

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
        //从主流获取侧输出流,打印
        process.getSideOutput(waterSensorOutputTag).printToErr("关窗后的迟到数据:");
        env.execute();
    }
}
