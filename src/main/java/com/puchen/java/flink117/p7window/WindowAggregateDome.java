package com.puchen.java.flink117.p7window;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
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
public class WindowAggregateDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("master", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //Aggregate聚合
        SingleOutputStreamOperator<String> aggregate = sersonWS.aggregate(
                /**
                 * 第一个类型 输入数据的类型
                 * 第二个类型 累加器的类型 存储的中间计算结果的类型
                 * 第三个类型 输出的类型
                 */
                new AggregateFunction<WaterSensor, Integer, String>() {
                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("创建累加器");
                        return 0;
                    }

                    /**
                     * 聚合逻辑
                     * 累一次调用一次
                     * @param waterSensor
                     * @param integer
                     * @return
                     */
                    @Override
                    public Integer add(WaterSensor waterSensor, Integer integer) {
                        System.out.println("调用add方法，value = "+ waterSensor);
                        return integer + waterSensor.getVc();
                    }

                    /**
                     * 获取最终的结果 窗口触发时候输出
                     * @param integer
                     * @return
                     */
                    @Override
                    public String getResult(Integer integer) {
                        System.out.println("调用add方法");
                        return integer.toString();
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        //只会会话窗口才会用到
                        return null;
                    }
                });
        aggregate.print();
        env.execute();

    }
}
