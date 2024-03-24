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
public class WaterMarksOutOfOrderDome2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 多并行度下的watermark传递
         * 1.接受到上游一个 取最小值
         * 2.往下游多个发送 广播
         */
//        env.setParallelism(2);
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval()
        
        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777)
                .map(new WaterSensorMapFunction());

        //指定wtareMarks
        //1.升序watermark策略
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                //升序的watermark 没有等待时间
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                //指定 时间戳分配器  从数据中提取
                .withTimestampAssigner((waterSensor,l) -> {
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
//        数据=WaterSensor{id='s1', ts=1, vc=1},recordTSA-9223372036854775808
//        数据=WaterSensor{id='s1', ts=2, vc=2},recordTSA-9223372036854775808
//        数据=WaterSensor{id='s1', ts=11, vc=1},recordTSA-9223372036854775808
//        数据=WaterSensor{id='s1', ts=12, vc=12},recordTSA-9223372036854775808
//        数据=WaterSensor{id='s1', ts=12, vc=12},recordTSA-9223372036854775808
//        数据=WaterSensor{id='s1', ts=13, vc=13},recordTSA-9223372036854775808
//        key=s1的窗口[1970-01-01 08:00:00.000,1970-01-01 08:00:10.000]包含2条数据====>[WaterSensor{id='s1', ts=1, vc=1}, WaterSensor{id='s1', ts=2, vc=2}]  注意这里 只输出2条数据  因为0-10这个窗口只有这两条数据

        /**
         *  内置watermark的生成原理
         *  1.都是走周期性生成 默认是200ms
         *  2.升序有序流 water = 当时最大的时间时间-1ms
         *  3.乱序流 water = 当前最大的时间时间 - 延迟时间 - 1ms
         */
    }
}
