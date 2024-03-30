package com.puchen.java.flink117.p10state;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 计算每种传感器的水位和
 *
 * @ClassName: KeyedValueStateDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/29 20:57
 * @Version: 1.0
 **/
public class TTLStateDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(((element, ts) -> element.getTs() * 1000L)));
        sensorDS.keyBy(r -> r.getId()).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        StateTtlConfig build = StateTtlConfig.newBuilder(Time.seconds(5))   //过期时间
//                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)   //装填 创建和写入 更新 过期时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  //不返回过期的状态
                                .build();

                        ValueStateDescriptor<Integer> lastVcStateDesc = new ValueStateDescriptor<>("lastVcState", Types.INT);
                        lastVcStateDesc.enableTimeToLive(build);
                        this.lastVcState = getRuntimeContext().getState(lastVcStateDesc);
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //先获取状态值 打印
                        Integer valueVc = lastVcState.value();
                        out.collect("key="+value.getId()+",状态值="+valueVc);
                        //更新状态值
                        if (value.getVc()>10) {
                            lastVcState.update(value.getVc());
                        }
                    }
                }
        ).print();

        env.execute();
    }

}
