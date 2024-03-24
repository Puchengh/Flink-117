package com.puchen.java.flink117.p7window;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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
public class TriggerAndEvictorWindowProcessDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));  //滚动窗口 窗口长度是10秒

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

/**
 * 触发器 移除器  现成的几个窗口 都有默认的视线 一般不需要自定义
 * 触发器(Trigger)
 *  1。窗口什么时候触发?
 *      时间的进展 >= 窗口的最大时间戳 end-1ms
 *         秒数是固定的时间  整秒
 *  2.窗口是怎么划分的：
 *          start=向下取证 取窗口长度的整数倍
 *          end=start + 窗口长度
 *          窗口是左闭右开
 *  *     public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
 *  *         final long remainder = (timestamp - offset) % windowSize;
 *             （13s-0）%10 = 3
 *             (27s-0）%10 = 7
 *
 *  *         // handle both positive and negative cases
 *  *         if (remainder < 0) {
 *  *             return timestamp - (remainder + windowSize);
 *  *         } else {
 *  *             return timestamp - remainder;
 *              13-3=10
 *              27-7=20
 *  *         }
 *  *     }
 * 移除器(Evictor)
 *
 * 3.窗口的生命周期？
 * 创建
 *      属于本窗口的第一条数据来的时候 现成new的 单例模式  放入一个单例的集合中
 * 销毁
 *      时间的进展大于等 串口的最大时间戳（end -1ms）+ 允许迟到的时候（默认 0） 和触发是两个动作
 */

    }
}
