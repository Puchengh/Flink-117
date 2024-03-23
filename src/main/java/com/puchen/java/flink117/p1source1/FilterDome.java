package com.puchen.java.flink117.p1source1;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.FilterFunctionImpl;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: FilterDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 10:06
 * @Version: 1.0
 **/
public class FilterDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<WaterSensor> sersorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)

        );
        SingleOutputStreamOperator<WaterSensor> sersorKS = sersorDS.filter(new FilterFunctionImpl("s1"));
        sersorKS.print();
        env.execute();
    }

}
