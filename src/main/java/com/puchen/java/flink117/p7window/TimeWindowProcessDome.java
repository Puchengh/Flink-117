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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WindowRduceDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 19:39
 * @Version: 1.0
 **/
public class TimeWindowProcessDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());
//        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));  //滚动窗口 窗口长度是10秒
//        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));  //滑动窗口
//        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));  //会话窗口 超时时间间隔5s
        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
            @Override
            public long extract(WaterSensor element) {
                //从数据中提交ts 作为时间间隔  动态间隔 动态覆盖提取时间 会话窗口 每条数据都会更新时间间隔
                return element.getTs() * 1000L;
            }
        }));

        SingleOutputStreamOperator<String> process = sersonWS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     * 全窗口 计算逻辑 只有窗口触发是才会调用一次 统一计算窗口的所有数据
                     * @param s The key for which this window is evaluated.  分组的可以
                     * @param context The context in which the window is being evaluated.  上下文
                     * @param elements The elements in the window being evaluated.  存的数据
                     * @param out A collector for emitting elements.  采集器
                     * @throws Exception
                     */
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

        //结果
        //key=s1的窗口[2024-03-24 11:11:50.000,2024-03-24 11:12:00.000]包含2条数据====>[WaterSensor{id='s1', ts=3, vc=3}, WaterSensor{id='s1', ts=5, vc=5}]

    }
}
