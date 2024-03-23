常见的物理分区策略有:
随机分配 轮训分配 重缩放 广播
        //随机分区  random.nextInt(numberOfChannels)(下游算子并行度)
//        master.shuffle().print();
        //rebalance 轮循 每个子任务都会轮循执行一次  nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels; 下游算子并行度
        //如果是数据倾斜的擦灰姑娘经 source读取进来之后调用rebalance 就可以解决数据源的数据倾斜
//        master.rebalance().print();
        //rescale 缩放  局部轮循 但是更加高效 比rebalance更加高效
//        master.rescale().print();
        //发送给下游所有子任务 broadcast 广播 给所有任务都给一分数据
//        master.broadcast().print();
        //只发送给第一个子任务分区
        master.global().print();
        env.execute();

        keyby  按照指定的key发送 相同的key发往同一个子任务

        one to one Forward分区

        总结: Flink提供了7中分区器 + 1中自定义的分区器