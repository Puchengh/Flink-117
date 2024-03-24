package com.puchen.java.flink117.p7window;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
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
public class WindowAggregateAndProcessDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /**
         * 事件驱动型
         */
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sersonWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        /**
         * Aggregate聚合 + 全窗口的process
         * 1.增量聚合函数处理数据 来一条计算一条
         * 2.窗口触发时候 增量聚合的结果（只有一条） 传递给全窗口函数
         * 3.经过全窗口函数的处理包装后 输出
         *
         * 结合两者的优点
         * 1.增量聚合：来一条计算一条 存储中间的计算结果 占用的空间少
         * 2.全窗口函数： 可以通过上下文 实现灵活的功能
         */

        SingleOutputStreamOperator<String> aggregate = sersonWS.aggregate(new MyAgg(),new MyProcess());
        aggregate.print();
        env.execute();

    }

    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String>{


        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }

        @Override
        public Integer add(WaterSensor waterSensor, Integer integer) {
            System.out.println("调用add方法，value = "+ waterSensor);
            return integer + waterSensor.getVc();
        }

        @Override
        public String getResult(Integer integer) {
            System.out.println("调用result方法");
            return integer.toString();
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            //只会会话窗口才会用到
            return null;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String,String,String,TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String windowStrat = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowend = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
            long count = elements.spliterator().estimateSize();

            out.collect("key=" + s + "的窗口[" + windowStrat + "," + windowend + "]包含" + count + "条数据====>" + elements.toString());
        }
    }
}
