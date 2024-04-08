package com.puchen.java.flink117.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class EnvDome {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);   //自动识别远程集群还是idea本地环境
//                .createRemoteEnvironment("10.107.105.8", 8081,"/opt/XXX")
        //流批一体:代码API是同一套 可以制定位 批  也可以指定位 流
        //一般不在代码写死 提交是可以参数指定:  -Dexecution.runtime-mode=BATCH
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env
//                .socketTextStream("10.107.105.8", 7777)
                .readTextFile("src/main/java/com/craiditx/flink/env/test.txt")
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT))

                .keyBy(value -> value.f0)
                .sum(1)
                .print();
        env.execute();  //触发执行程序
            //1.默认 env.execute()触发一个flink job
                    //一个main方法可以调用多个xectue  但是没有意义 执行到第一个就会阻塞住
            //2.env.executeAsync() 异步触发 不阻塞
                    //一个main方法里面executeAsync个数 = 生成的flin job数
                //yarn-application 集群提交一次 取决于 调用了n个executeAsync
                //对应application集群里面会有n个job
                //对应Jobmanager当中，会有n个JobManager
        env.executeAsync();  //异步处理程序
    }
}
