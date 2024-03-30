package com.puchen.java.flink117.p10state;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: OperatorBroadcastStateDemo
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/30 12:56
 * @Version: 1.0
 **/
public class SetBackendDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        /**
         * TODO 状态后端
         * 1、负责管理 本地状态
         * 2、 hashmap
         *          存在 TM的 JVM的堆内存，  读写快，缺点是存不了太多（受限与TaskManager的内存）
         *     rocksdb
         *          存在 TM所在节点的rocksdb数据库，存到磁盘中，  写--序列化，读--反序列化
         *          读写相对慢一些，可以存很大的状态
         *
         * 3、配置方式
         *    1）配置文件 默认值  flink-conf.yaml
         *    2）代码中指定
         *    3）提交参数指定
         *    flink run-application -t yarn-application
         *    -p 3
         *    -Dstate.backend.type=rocksdb
         *    -c 全类名
         *    jar包
         */
            //1.指定状态后端
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend);
        //2.使用状态后端rocksdb
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        env.setStateBackend(embeddedRocksDBStateBackend);

        // 数据流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("slave2", 7777)
                .map(new WaterSensorMapFunction());

        // 配置流（用来广播配置）
        DataStreamSource<String> configDS = env.socketTextStream("slave2", 8888);

        // TODO 1. 将 配置流 广播  带有广播状态的广播流
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = configDS.broadcast(broadcastMapState);

        // TODO 2.把 数据流 和 广播后的配置流 connect
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(configBS);

        // TODO 3.调用 process
        sensorBCS
                .process(
                        new BroadcastProcessFunction<WaterSensor, String, String>() {
                            /**
                             * 数据流的处理方法： 数据流 只能 读取 广播状态，不能修改
                             * @param value
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                // TODO 5.通过上下文获取广播状态，取出里面的值（只读，不能修改）
                                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                                Integer threshold = broadcastState.get("threshold");
                                // 判断广播状态里是否有数据，因为刚启动时，可能是数据流的第一条数据先来
                                threshold = (threshold == null ? 0 : threshold);
                                if (value.getVc() > threshold) {
                                    out.collect(value + ",水位超过指定的阈值：" + threshold + "!!!");
                                }

                            }

                            /**
                             * 广播后的配置流的处理方法:  只有广播流才能修改 广播状态
                             * @param value
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                                // TODO 4. 通过上下文获取广播状态，往里面写数据
                                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                                broadcastState.put("threshold", Integer.valueOf(value));

                            }
                        }

                )
                .print();

        env.execute();
    }
}
