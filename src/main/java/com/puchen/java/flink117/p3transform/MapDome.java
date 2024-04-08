package com.puchen.java.flink117.p3transform;
import com.puchen.java.flink117.FunctionUtil.FunctionUtilImpl;
import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sersor = env.fromElements(
                new WaterSensor("zhangsan", 1L, 4),
                new WaterSensor("lisi", 2L, 5),
                new WaterSensor("wangwu", 3L, 6)
        );
        //map算子 :一进一出
        //方式1
//        SingleOutputStreamOperator<String> map = sersor.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//        });
        //方式2 lambda表达式
//        SingleOutputStreamOperator<String> map = sersor.map(sen -> sen.getId());
        //方式2
        //定义一个类来实现MapFunction
        SingleOutputStreamOperator<String> map = sersor.map(new FunctionUtilImpl());
        map.print();
        env.execute();
    }
}
