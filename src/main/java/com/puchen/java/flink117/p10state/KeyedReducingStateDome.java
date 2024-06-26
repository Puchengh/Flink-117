package com.puchen.java.flink117.p10state;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * 计算每种传感器的水位和
 *
 * @ClassName: KeyedValueStateDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/29 20:57
 * @Version: 1.0
 **/
public class KeyedReducingStateDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(((element, ts) -> element.getTs() * 1000L)));
        sensorDS.keyBy(r -> r.getId()).process(new KeyedProcessFunction<String, WaterSensor, String>() {

                                                   ReducingState<Integer> vcSumReducingState;

                                                   @Override
                                                   public void open(Configuration parameters) throws Exception {
                                                       super.open(parameters);
                                                       vcSumReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("vcSumReducingState", new ReduceFunction<Integer>() {
                                                           @Override
                                                           public Integer reduce(Integer value1, Integer value2) throws Exception {
                                                               return value1 + value2;
                                                           }
                                                       }, Types.INT));
                                                   }

                                                   @Override
                                                   public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                                       // 来一条数据  添加到reducing状态里面
                                                       vcSumReducingState.add(value.getVc());
                                                       Integer i = vcSumReducingState.get();
                                                       out.collect("传感器id为" + value.getId() + ",水为之总和=" + i);

//                                                       vcSumReducingState.get();
//                                                       vcSumReducingState.add();
//                                                       vcSumReducingState.clear();
                                                   }
                                               }

        ).print();

        env.execute();
    }

}
