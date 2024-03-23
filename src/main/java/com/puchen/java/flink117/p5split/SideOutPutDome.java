package com.puchen.java.flink117.p5split;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: SideOutPutDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 14:31
 * @Version: 1.0
 **/
public class SideOutPutDome {

    public static void main(String[] args) throws Exception {
        /**
         * 1.使用process算子
         * 2.定义OutPutTag对象
         * 3.调用ctx。out
         * 4.通过getSideOutput获取测流
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        SingleOutputStreamOperator<WaterSensor> master = env.socketTextStream("master", 7777).map(new WaterSensorMapFunction());
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        /**
         * 上下文调用outpt 将数据放入侧输出流
         * 第一个参数 Tag对象
         * 第二个参数 放入侧输出流中的数据
         */
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        /**
         * 上下文调用outpt 将数据放入侧输出流
         * 第一个参数 Tag对象
         * 第二个参数 放入侧输出流中的数据
         */

        SingleOutputStreamOperator<WaterSensor> process = master.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                if ("s1".equals(id)) {
                    //s1放到测流中


                    ctx.output(s1Tag, value);
                } else if ("s2".equals(id)) {
                    //s2放到测流中


                    ctx.output(s2Tag, value);
                } else {
                    //非s1 s2其他的流
                    out.collect(value);
                }

            }  //定义的主流的输出类型
        });
        //打印主流
//        process.print();
        //打印测流
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);
        /**
         * 侧输出流
         * watersensor 分出s1和s2的数据
         */
        s1.print();
        s2.print();
        env.execute();
// 结果:
//        {1=[(1,aa1,1)]}
//        2> s1:(1,a1)<=======>s2:(1,aa1,1)
//        {1=[(1,a1)]}
//        2> s1:(1,a1)<=======>s2:(1,aa2,2)
//        {1=[(1,aa1,1), (1,aa2,2)]}
//        2> s1:(1,a2)<=======>s2:(1,aa1,1)
//        2> s1:(1,a2)<=======>s2:(1,aa2,2)
//        {1=[(1,a1), (1,a2)]}
//        {1=[(1,aa1,1), (1,aa2,2)], 2=[(2,bb,1)]}
//        2> s1:(2,b)<=======>s2:(2,bb,1)
//        {1=[(1,a1), (1,a2)], 2=[(2,b)]}
//        {1=[(1,aa1,1), (1,aa2,2)], 2=[(2,bb,1)], 3=[(3,cc,1)]}
//        2> s1:(3,c)<=======>s2:(3,cc,1)
//        {1=[(1,a1), (1,a2)], 2=[(2,b)], 3=[(3,c)]}
    }
}
