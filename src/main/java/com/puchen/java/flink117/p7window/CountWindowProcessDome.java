package com.puchen.java.flink117.p7window;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WindowRduceDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 19:39
 * @Version: 1.0
 **/
public class CountWindowProcessDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());
//        WindowedStream<WaterSensor, String, GlobalWindow> sersonWS = sensorKS.countWindow(5);  //滚动窗口 五条数据满足一个窗口
        WindowedStream<WaterSensor, String, GlobalWindow> sersonWS = sensorKS.countWindow(5,2);  //滑动窗口 窗口5条  步长为2条 每经过一个步长都有一个窗口触发  丢一次触发是在第二条数据来的时候

        SingleOutputStreamOperator<String> process = sersonWS.process(
                new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long maxTs = context.window().maxTimestamp();
                        long count = elements.spliterator().estimateSize();
                        String windowTime = DateFormatUtils.format(maxTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("key=" + s + "的窗口最大时间=" + windowTime +  ",包含" + count + "条数据====>" + elements.toString());

                    }
                }
        );
        process.print();
        env.execute();

        //结果
        //key=s1的窗口[2024-03-24 11:11:50.000,2024-03-24 11:12:00.000]包含2条数据====>[WaterSensor{id='s1', ts=3, vc=3}, WaterSensor{id='s1', ts=5, vc=5}]

    }
}
