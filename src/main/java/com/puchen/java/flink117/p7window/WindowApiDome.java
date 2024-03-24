package com.puchen.java.flink117.p7window;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @ClassName: WindowApiDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 19:06
 * @Version: 1.0
 **/
public class WindowApiDome {

    /**
     * 基于按键分区的数据流keyedStream来开创，数据会被分为多天逻辑流，这就是keyedStream,窗口会在多个并行子任务上同时执行
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("master", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());

        //1.指定窗口的分配器 ---时间 or 计数？ 滚动 滑动  会话？
        //1.1没有keyby的窗口 窗口内的所有数据进入同一个子任务 并行度只能为1
//        sensorKS.windowAll();
        //1.2有keby的窗口 每个key上都定义了一组窗口 各自独立的进行统计计算

        //基于时间的
        WindowedStream<WaterSensor, String, TimeWindow> window = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));   //滚动窗口 10s
        WindowedStream<WaterSensor, String, TimeWindow> window1 = sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));//滑动窗口 10s  步长为2s
        WindowedStream<WaterSensor, String, TimeWindow> window2 = sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));  //会话窗口 超时时间间隔5s

        //基于计数的
        WindowedStream<WaterSensor, String, GlobalWindow> waterSensorStringGlobalWindowWindowedStream = sensorKS.countWindow(5);  //滚动窗口 窗口长度=5个元素
        WindowedStream<WaterSensor, String, GlobalWindow> waterSensorStringGlobalWindowWindowedStream1 = sensorKS.countWindow(5, 2);  //滑动窗口 窗口长度=5s 步长 =2s
        WindowedStream<WaterSensor, String, GlobalWindow> window3 = sensorKS.window(GlobalWindows.create());  //全局窗口 计数窗口的底层就是用这个 需要自定义触发器 很少用


        //2.指定窗口函数 窗口内数据的计算逻辑

        //增量聚合: 来一条数据 计算一条数据窗口触发的时候输出计算结果。
//        window.reduce().aggregate();
        //全窗口函数: 数据来了不计算，存起来，窗口触发的时候，计算并且输出结果。
//        window.process();
        env.execute();
    }

}
