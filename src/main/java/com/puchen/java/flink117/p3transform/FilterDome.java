package com.puchen.java.flink117.p3transform;

import com.puchen.java.flink117.bean.WaterSensor;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sersorDS = env.fromElements(
                new WaterSensor("zhangsan", 1L, 4),
                new WaterSensor("lisi", 2L, 5),
                new WaterSensor("lisi", 11L, 12),
                new WaterSensor("wangwu", 3L, 6)
        );
        //filter算子 :一进一出
        SingleOutputStreamOperator<WaterSensor> filter = sersorDS.filter(seror -> "lisi".equals(seror.getId()));
//        SingleOutputStreamOperator<WaterSensor> filter = sersorDS.filter(new FilterFunction<WaterSensor>() {
//            @Override
//            public boolean filter(WaterSensor value) throws Exception {
//                return "lisi".equals(value.getId());
//            }
//        });

        filter.print();
        env.execute();
    }
}
