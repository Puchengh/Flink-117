package com.puchen.java.flink117.p5split;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: ConnectKeByDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 15:19
 * @Version: 1.0
 **/
public class ConnectKeByDome {
    public static void main(String[] args) throws Exception {
        /**
         * 链接两条流
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);
        /**
         * 多并行度下需要根据关联调酒keyby 才能保证key相同的数据才能key相同的数据到一起去 才能匹配上
         *
         */
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKeyby = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);

        /**
         * 要实现互相匹配的效果
         * 1.两条流 不一定谁的数据先来
         * 2.每条流 有数据来 存在一个变量中 存在hashMap  key--id   value--list
         * 3.每条流有数据来的时候 除了存变量中  不知道对方是否有匹配上的数据 取另外一条流存到变量中查找是否有匹配上
         */
        SingleOutputStreamOperator<String> process = connectKeyby.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {

            Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

            /**
             * 第一条流的处理逻辑
             * @param value The stream element  第一条流的数据
             * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
             *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
             *     timers and querying the time. The context is only valid during the invocation of this
             *     method, do not store it.  上下文
             * @param out The collector to emit resulting elements to  采集器
             * @throws Exception
             */
            @Override
            public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {

                Integer id = value.f0;
                //s1的数据来了就存在变量中
                if (!s1Cache.containsKey(id)) {
                    //如果key不存在 说明该key是第一条数据 初始化 put到map中
                    List<Tuple2<Integer, String>> s1value = new ArrayList<>();
                    s1value.add(value);
                    s1Cache.put(id, s1value);

                } else {
                    //key存在 不是key的第一条数据 直接添加到value的list中
                    s1Cache.get(id).add(value);
                }

                //存完之后 去s2Cache中查找是否能匹配上  匹配上就输出 匹配不上就不输出
                if (s2Cache.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                        out.collect("s1:" + value + "<=======>" + "s2:" + s2Element);
                    }

                }
                System.out.println(s1Cache);
            }

            /**
             * 第二条流的处理逻辑
             * @param value The stream element  第二条流的数据
             * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
             *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
             *     timers and querying the time. The context is only valid during the invocation of this
             *     method, do not store it.  上下文
             * @param out The collector to emit resulting elements to  采集器
             * @throws Exception
             */
            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<String> out) throws Exception {

                Integer id = value.f0;
                //s1的数据来了就存在变量中
                if (!s2Cache.containsKey(id)) {
                    //如果key不存在 说明该key是第一条数据 初始化 put到map中
                    List<Tuple3<Integer, String, Integer>> s2value = new ArrayList<>();
                    s2value.add(value);
                    s2Cache.put(id, s2value);

                } else {
                    //key存在 不是key的第一条数据 直接添加到value的list中
                    s2Cache.get(id).add(value);
                }

                //存完之后 去s2Cache中查找是否能匹配上  匹配上就输出 匹配不上就不输出
                if (s1Cache.containsKey(id)) {
                    for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                        out.collect("s1:" + s1Element + "<=======>" + "s2:" + value);
                    }

                }

                System.out.println(s2Cache);
            }
        });


        process.print();
        env.execute();
    }

}
