package com.puchen.java.flink117.p2aggreagte;

import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: ReduceDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 9:50
 * @Version: 1.0
 **/
public class ReduceDome {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<WaterSensor> sersorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)

        );
        KeyedStream<WaterSensor, String> sersorKS = sersorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        /**
         * 1.key之后调用
         * 2.输入类型 = 输出类型 类型不能变
         * 3.每个key的第一条数据来的时候 不会执行reduce方法 存起来直接输出
         * 4、reduce方法中的两个参数  --flink的特定 有状态的计算
         *  value1 之间的计算结果 存状态
         *  value2 现在来的数据
         */
        SingleOutputStreamOperator<WaterSensor> reduce = sersorKS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("value1=" + value1);
                System.out.println("value2=" + value2);
                return new WaterSensor(value1.id, value1.ts, value1.vc + value2.vc);
            }
        });
        reduce.print();
        env.execute();
    }
}
