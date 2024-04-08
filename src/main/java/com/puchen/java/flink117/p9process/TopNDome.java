package com.puchen.java.flink117.p9process;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import com.puchen.java.flink117.bean.WaterSensor;

public class TopNDome {
    /**
     * 案例需求: 实时统计一段时间内的出现次数最多的水位，例如：统计最近10秒内出现次数最多的两个水位
     * 并且每5秒更新一次。 可以用一次滑动窗口来实现。 于是就需要开滑动窗口收集传感器的数据，按照不同的水位
     * 进行统计，而后汇总排序并最终输出前两名。 就是是TopN的问题
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("10.107.105.8", 7777).map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.parseInt(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                                                       <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                                       .withTimestampAssigner(((element, recordTimestamp) -> {
                                                           return element.getTs() * 1000L;
                                                       })));
        //最近10秒=窗口长度  每5秒输出 = 滑动步长
        //思路一 所有数据到一起 用hashmap存储 key=vc value=count值

        //TODO
//        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
//                        .process(new MyProFunction())

        env.execute();
    }

    public static class MyProFunction extends ProcessAllWindowFunction<WaterSensor,Integer, Window> {

        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, Integer, Window>.Context context, Iterable<WaterSensor> elements, Collector<Integer> out) throws Exception {

        }
    }



}
