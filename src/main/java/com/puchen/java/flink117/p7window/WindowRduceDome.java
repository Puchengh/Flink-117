package com.puchen.java.flink117.p7window;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @ClassName: WindowRduceDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 19:39
 * @Version: 1.0
 **/
public class WindowRduceDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("master", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //增量聚合的reduce
        /**
         * 窗口的reduce
         * 1.相同key的第一条数据来的额时候 不会调用reduce方法
         * 2.增量聚合： 来一条数据 就会计算一次 但是会不会输出
         * 3.在窗口触发的时候 才会输出窗口的最终计算结果
         */
        SingleOutputStreamOperator<WaterSensor> reduce = sersonWS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                System.out.println("调用reduce方法,value1=" + waterSensor + "value2=" + t1);
                return new WaterSensor(waterSensor.getId(), t1.getTs(), +waterSensor.getVc() + t1.getVc());
            }
        });
        reduce.print();
        env.execute();

    }
}
