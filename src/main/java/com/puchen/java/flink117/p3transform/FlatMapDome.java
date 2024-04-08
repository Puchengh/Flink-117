package com.puchen.java.flink117.p3transform;

import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sersorDS = env.fromElements(
                new WaterSensor("zhangsan", 1L, 4),
                new WaterSensor("lisi", 2L, 5),
                new WaterSensor("lisi", 11L, 12),
                new WaterSensor("wangwu", 3L, 6)
        );
        //一进多出
        /**
         * map怎么控制的一进一出 因为他使用的是return
         * flatMap怎么空值的一进多出,通过clooection 调用几次就生成几个
         */
        SingleOutputStreamOperator<String> flatMap = sersorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("lisi".equals(value.getId())) {
                    out.collect(value.getVc().toString());
                } else if ("wangwu".equals(value.getId())) {
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }

            }
        });

        flatMap.print();
        env.execute();
    }
}
