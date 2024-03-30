package com.puchen.java.flink117.p10state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: OperatorListStateDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/30 12:42
 * @Version: 1.0
 **/
public class OperatorListStateDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env
                .socketTextStream("slave2", 7777)
                .map(new MyCountMapFunction())
                .print();


        env.execute();
    }


    // TODO 1.实现 CheckpointedFunction 接口
    public static class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;


        @Override
        public Long map(String value) throws Exception {
            return ++count;
        }

        /**
         * TODO 2.本地变量持久化：将 本地变量 拷贝到 算子状态中,开启checkpoint时才会调用
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            // 2.1 清空算子状态
            state.clear();
            // 2.2 将 本地变量 添加到 算子状态 中
            state.add(count);
        }

        /**
         * TODO 3.初始化本地变量：程序启动和恢复时， 从状态中 把数据添加到 本地变量，每个子任务调用一次
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            // 3.1 从 上下文 初始化 算子状态
            state = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("state", Types.LONG));

            // 3.2 从 算子状态中 把数据 拷贝到 本地变量
            if (context.isRestored()) {
                for (Long c : state.get()) {
                    count += c;
                }
            }
        }
    }

}
/**
 * 算子状态中 list和unionlist的区别 并行度改变时候 怎么重新分配状态
 * 1。list状态 轮循均分给新的并行子任务
 * 2.uninlist 状态 原先的多个子任务的装填 合并成一份完整的 会把完整的列表广播给信的并行子任务（没人一份完整的）
 */
