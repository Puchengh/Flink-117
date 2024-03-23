package com.puchen.java.flink117.p4physical_partitionsing;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: PartitionDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 10:39
 * @Version: 1.0
 **/
public class PartitionDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<String> master = env.socketTextStream("master", 7777);
        //随机分区  random.nextInt(numberOfChannels)(下游算子并行度)
//        master.shuffle().print();
        //rebalance 轮循 每个子任务都会轮循执行一次  nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels; 下游算子并行度
        //如果是数据倾斜的擦灰姑娘经 source读取进来之后调用rebalance 就可以解决数据源的数据倾斜
//        master.rebalance().print();
        //rescale 缩放  局部轮循 但是更加高效 比rebalance更加高效
//        master.rescale().print();
        //发送给下游所有子任务 broadcast 广播 给所有任务都给一分数据
//        master.broadcast().print();
        //只发送给第一个子任务分区  return 0;
        master.global().print();
        env.execute();
    }

}
