package com.puchen.java.flink117.p10state;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 计算每种传感器的平均水位
 *
 * @ClassName: KeyedValueStateDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/29 20:57
 * @Version: 1.0
 **/
public class KeyedAggregatingStateDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(((element, ts) -> element.getTs() * 1000L)));
        sensorDS.keyBy(r -> r.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    AggregatingState<Integer,Double> vcAggState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>(
                                "vcAggState",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0,0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0+value,accumulator.f1+1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return accumulator.f0 * 1D/accumulator.f1;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
//                                        return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
                                        return null;
                                    }
                                },
                                Types.TUPLE(Types.INT, Types.INT)

                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //将水位值添加到聚合状态中
                        vcAggState.add(value.getVc());
                        Double vc = vcAggState.get();
                        out.collect("传感器id为"+value.getId()+",平均水位值="+vc);

//                        vcAggState.get();
//                        vcAggState.add();
//                        vcAggState.clear();

                    }

                }
        ).print();

        env.execute();
    }

}
