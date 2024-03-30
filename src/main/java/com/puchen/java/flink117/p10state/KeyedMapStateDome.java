package com.puchen.java.flink117.p10state;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 统计每种水位值出现的次数
 *
 * @ClassName: KeyedValueStateDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/29 20:57
 * @Version: 1.0
 **/
public class KeyedMapStateDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(((element, ts) -> element.getTs() * 1000L)));
        sensorDS.keyBy(r -> r.getId()).process(


                new KeyedProcessFunction<String, WaterSensor, String>() {
                    MapState<Integer, Integer> vcMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vcMapState", Types.INT, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //判断是否存在对应的key
                        if (vcMapState.contains(value.getVc())) {
                            Integer i = vcMapState.get(value.getVc());
                            vcMapState.put(value.getVc(), ++i);
                            i += 1;
                        }
                        //如果不包含
                        else {
                            vcMapState.put(value.getVc(), 1);
                        }

                        StringBuffer stringBuffer = new StringBuffer();
                        stringBuffer.append("传感器id为" + value.getId() + "\n");

                        for (Map.Entry<Integer, Integer> entry : vcMapState.entries()) {
                            stringBuffer.append(entry.toString() + "\n");
                        }
                        stringBuffer.append("========================");
                        out.collect(stringBuffer.toString());


//                        vcMapState.get();
//                        vcMapState.contains();
//                        vcMapState.put();
//                        vcMapState.putAll();
//                        vcMapState.entries();
//                        vcMapState.keys();
//                        vcMapState.values();
//                        vcMapState.isEmpty();
//                        vcMapState.iterator();
//                        vcMapState.clear();
                    }


                }

        ).print();

        env.execute();
    }

}
