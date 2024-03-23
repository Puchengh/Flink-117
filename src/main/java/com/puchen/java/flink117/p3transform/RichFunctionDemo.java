package com.puchen.java.flink117.p3transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: RichFunctionDemo
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 10:09
 * @Version: 1.0
 **/
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
//        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);
//        env.setParallelism(2);
//
//        /**
//         * RichXXXXFunction 富函数
//         * 1.多个声明周期管理方法
//         *  open 每个子任务 在启动时调用一次
//         *  close 每个子任务 在结束时调用一次
//         *   如果shiflink异常挂掉 不会调用close  如果是正常调用cancle 可以close
//         * 2.多个一个运行时上下文
//         * 可以获取一些运行时的环境信息 比如 子任务编号，名称，其他的信息
//         */
//        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                RuntimeContext runtimeContext = getRuntimeContext();
//                System.out.println(
//                        "子任务编号=" + runtimeContext.getIndexOfThisSubtask()
//                                + ", 子任务名称" + runtimeContext.getTaskNameWithSubtasks()
//                                + "opne()调用");
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//                RuntimeContext runtimeContext = getRuntimeContext();
//                System.out.println(
//                        "子任务编号=" + runtimeContext.getIndexOfThisSubtask()
//                                + ", 子任务名称" + runtimeContext.getTaskNameWithSubtasks()
//                                + "close()调用");
//            }
//
//            @Override
//            public Integer map(Integer value) throws Exception {
//                return value + 1;
//            }
//        });
//        map.print();


        DataStreamSource<String> master = env.socketTextStream("master", 7777);
        env.setParallelism(2);
        SingleOutputStreamOperator<Integer> mapSo = master.map(new RichMapFunction<String, Integer>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext runtimeContext = getRuntimeContext();
                System.out.println(
                        "子任务编号=" + runtimeContext.getIndexOfThisSubtask()
                                + ", 子任务名称" + runtimeContext.getTaskNameWithSubtasks()
                                + "opne()调用");
            }

            @Override
            public void close() throws Exception {
                super.close();
                RuntimeContext runtimeContext = getRuntimeContext();
                System.out.println(
                        "子任务编号=" + runtimeContext.getIndexOfThisSubtask()
                                + ", 子任务名称" + runtimeContext.getTaskNameWithSubtasks()
                                + "  close()调用");
            }

            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value) + 1;
            }
        });

        mapSo.print();
        env.execute();
    }
}
