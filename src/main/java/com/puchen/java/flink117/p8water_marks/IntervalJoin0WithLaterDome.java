package com.puchen.java.flink117.p8water_marks;

import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;

/**
 * @ClassName: WindowJoinDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/25 20:32
 * @Version: 1.0
 **/
public class IntervalJoin0WithLaterDome {

    public static void main(String[] args) throws Exception {
        /**
         *双流联结
         * 1,只支持时间时间
         * 2.指定上下界的偏移 负号代表时间往前 正号带边时间往后
         * 3.process中 朱能处理join上的数据
         * 4.两条流关联后的watermark 一两条流中最小的为准
         * 5.如果当前数据的时间时间 < 当前的watermark 就是迟到数据 主流的process不处理
         *  ==> between后可以指定将左流或者右流的迟到数据 放入到侧输出流
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.socketTextStream("slave2", 7777)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return Tuple2.of(datas[0],Integer.parseInt(datas[1]));
                    }
                })

                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.socketTextStream("slave2", 8888)
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return Tuple3.of(datas[0],Integer.parseInt(datas[1]),Integer.parseInt(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );

        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);

        OutputTag<Tuple2<String, Integer>> ks1LateTag = new OutputTag<>("ks1-late", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> ks2LateTag = new OutputTag<>("ks2" +
                "-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2) //将ks1的迟到数据放入到侧输出流
                .between(Time.seconds(-2), Time.seconds(2))   //将ks2的迟到数据放入到侧输出流
                .sideOutputLeftLateData(ks1LateTag)
                .sideOutputRightLateData(ks2LateTag)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * 进入这个方法 两条流的数据匹配上 才会调用这个方法
                     * @param left The left element of the joined pair.
                     * @param right The right element of the joined pair.
                     * @param ctx A context that allows querying the timestamps of the left, right and joined pair.
                     *     In addition, this context allows to emit elements on a side output.
                     * @param out The collector to emit resulting elements to.
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "<----->" + right);
                    }
                });
        process.print();
        process.getSideOutput(ks1LateTag).print("ks1迟到的数据");
        process.getSideOutput(ks2LateTag).print("ks2迟到的数据");

        env.execute();
    }
}
