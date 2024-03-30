package com.puchen.java.flink117.p9process;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @ClassName: KeyedPProcessFunctionTopNDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/27 21:12
 * @Version: 1.0
 **/
public class KeyedPProcessFunctionTopNDome {

    public static void main(String[] args) throws Exception {
        /**
         * 1.按照vn做key  开窗 分别count
         *          增量聚合 计算count
         *          全窗口 对计算结果count值封装 带上窗口结束时间的标签
         *                  为了让同一窗口时间范围的计算结果到一起去
         * 2.对同一个窗口方位的count值进行处理 排序 取前N个
         *   按照windowEnd做key
         *   使用process 来一提哦啊哦调用一次 需要先存 分开存HashMap key = windowEnd value=list
         *   使用定时器 对存起来的结果 进行排序 取前N个
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(((element, ts) -> element.getTs() * 1000L))
                );
        //思路二 使用keyedProcessFunction实现

        //1.按照vc分组 开窗 聚合（增量计算+全量打标签）
        //因为窗口聚合后 就是普通的流 没了窗口信息 需要自己打上窗口的标记 windowend

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowAgg = sensorDS.keyBy(sensor -> sensor.getVc())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new VcCountAgg(),
                        new WindowResult());
        //按照窗口标签（窗口结束时间）keyby 保证同一个窗口时间范围的结束 到一起去 排序 取TOPN
        windowAgg.keyBy(r -> r.f2)
                .process(new TopN(2)).print();
        env.execute();
    }


    public static class VcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    /**
     * 泛型如下
     * 第一个: 输入类型 = 增量行数的输出 count值 Integer
     * 第二个： 输出类型 = Tuple3<vc,count,windoEnd>  带上窗口结束时间的标签
     * 第三个: key类型 vc Integer
     * 第四个: 窗口函数
     */
    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {
        @Override
        public void process(Integer key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            //迭代器里面只有一条数据 next一次即可
            Integer count = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            out.collect(Tuple3.of(key, count, windowEnd));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        //存不同窗口的统计结果 key=windowEnd value=list数据
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataListMap;
        //要去的top数量
        private int threshold;

        public TopN(int threshold) {
            this.threshold = threshold;
            dataListMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            //进入这个方法的只是一条数据 要排序 得到齐才能 ===> 存起来  不同窗口分开存
            //1.存到hashMap中
            Long windowEnd = value.f2;
            if (dataListMap.containsKey(windowEnd)) {
                //1.1包含vc  不是该vc的第一条 直接添加到list中
                List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
                dataList.add(value);
            } else {
                //1.1包含vc  不是该vc的第一条 初始化list中
                List<Tuple3<Integer, Integer, Long>> dataListInit = new ArrayList<>();
                dataListInit.add(value);
                dataListMap.put(windowEnd, dataListInit);

            }
            //注册一个定时器  windowEnd + 1ms即可 （同一个窗口范围，应该同时输出）
            //同一个窗口范围 应该同时输出 只不过是一条一条调用processElement方法 只需要延迟1ms即可
            ctx.timerService().registerEventTimeTimer(windowEnd + 1);


        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            //定时器触发 同一个窗口方位的计算结果攒齐了 开始排序 取ToN
            Long windowEnd = ctx.getCurrentKey();
            //1.排序
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            dataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    return o2.f1 - o1.f1;
                }
            });


            //2.取topN
            StringBuffer ouStr = new StringBuffer();
            ouStr.append("======================================\n");

            for (int i = 0; i < Math.min(threshold, dataList.size()); i++) {
                //遍历 排序后的list 取出前threshold个 考虑可能list不够2个的情况 ==> List中元素的个数 和2 取最小值
                Tuple3<Integer, Integer, Long> vcCount = dataList.get(i);
                ouStr.append("Top" + (i + i) + "\n");
                ouStr.append("vc=" + vcCount.f0 + "\n");
                ouStr.append("count=" + vcCount.f1 + "\n");
                ouStr.append("窗口结束时间=" + vcCount.f2+ "\n");
                ouStr.append("======================================\n");
            }

            //用完的List 即使清理 节省资源
            dataList.clear();

            out.collect(ouStr.toString());
        }

    }

}
