package com.puchen.java.flink117.p0dome;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCountStreamUnbondedDemo
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/17 16:59
 * @Version: 1.0
 **/
public class WordCountStreamUnbondedDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDS = env.socketTextStream("192.168.116.130", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                        out.collect(wordsAndOne);
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                //Flink 具有一个类型提取系统，可以分析函数的输入和返回类型，自动获取类型信息，从而获得对应的序列化器和反序列化器。但是，
                // 由于 Java 中泛型擦除的存在，在某些特殊情况下（如 Lambda 表达式中），自动提取的信息是不够精细的，需要显式地提供类型信息，才能使应用程序正常工作或提高其性能

                .keyBy((Tuple2<String, Integer> value) -> value.f0).sum(1);
        sum.print();
        env.execute();
    }

}
