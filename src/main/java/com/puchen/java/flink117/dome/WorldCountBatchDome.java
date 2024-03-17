package com.puchen.java.flink117.dome;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WorldCountBatchDome
 * @Desc: DataSet实现wordCount(不推荐)
 * @Author: puchen
 * @Date: 2024/3/17 15:50
 * @Version: 1.0
 **/
public class WorldCountBatchDome {

    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据 从文件中读取
        DataSource<String> lineDS = env.readTextFile("E:\\study\\Flink-117\\input\\word.txt");
        //3.切分 转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //3.1切分 转换
                String[] words = value.split(" ");
                //3.2 将单词转换为 (word，1)
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    //3.3 使用collection 向下游发送数据
                    out.collect(wordTuple2);
                }
            }
        });

        //4.按照word分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroup = wordAndOne.groupBy(0);
        //5.各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroup.sum(1);   //这里的1是位置 表示第二个元素

        //6.输出
        sum.print();

    }
}
