package com.puchen.java.flink117.p2aggreagte;

import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: KeyByDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 9:15
 * @Version: 1.0
 **/
public class SimpleAggregateDome {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sersorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 12L, 11),
                new WaterSensor("s1", 11L, 10),
                new WaterSensor("s1", 9L, 8),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)

        );
        //按照id分组
        /**
         * 1.返回的是一个 KeyedStream 键控流
         * 2.keyby不是一个转换算子 只是对数据进行重分区，不能设置并行度 是有规律的重分区
         * 3.在这里会体现一个hash
         * 4.keyby 分区和分组的关系
         *      1)keyby是对数据分组 保证 相同的key的数据 在同个分区
         *      2)分区 一个子任务 可以理解为一个分区  一个分区（子任务）中可以存在多个组
         *      3）如果有3个组 只有2个分组  那么会通过hash取值存在在对应的分区里面
         */
        KeyedStream<WaterSensor, String> sersorKS = sersorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        /**
         * 简单聚合算子
         * 1.keyby之后才能调用
         * 2.分组内的聚合 对同一个key的数据进行聚合
         */

        //传位置索引的值使用Tuple类型  不适用于POJO类型
        /**
         * maxhe maxby的区别
         * max: 只会去比较字段的最大值  非比较字段保留第一次的值
         * maxby： 取比较字段的最大值 同时非比饺字段去最大值这条数据的值
         */
//        SingleOutputStreamOperator<WaterSensor> sum = sersorKS.sum(2);
//        SingleOutputStreamOperator<WaterSensor> sum = sersorKS.sum("vc");
//        SingleOutputStreamOperator<WaterSensor> min = sersorKS.min("vc");
        SingleOutputStreamOperator<WaterSensor> maxby = sersorKS.maxBy("vc");
        maxby.print();

        env.execute();
    }

}
