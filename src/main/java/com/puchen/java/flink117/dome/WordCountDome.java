package com.puchen.java.flink117.dome;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCountDome
 * @Desc: DataStream 实现wordcount : 读文件（有界流）
 * @Author: puchen
 * @Date: 2024/3/17 16:35
 * @Version: 1.0
 **/
public class WordCountDome {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据 从文件中读取数据
        DataStreamSource<String> lineDs = env.readTextFile("E:\\study\\Flink-117\\input\\word.txt");
        //3.处理数据,切分 转换 分组 聚合
        //3.1 切分转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格切分
                String[] words = value.split(" ");
                for (String word : words) {
                    //转换成二元组(word,1)
                    Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                    //通过采集器 向下游发送数据
                    out.collect(wordsAndOne);
                }
            }
        });
        //3.2分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //3.3聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOneKS.sum(1);

        //4.输出数据
        result.print();
        //5.执行 类似sparkstreaming最后的 ss.start()
        env.execute();
    }


}
