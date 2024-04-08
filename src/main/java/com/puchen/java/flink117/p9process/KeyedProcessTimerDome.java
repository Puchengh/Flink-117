package com.puchen.java.flink117.p9process;

import java.time.Duration;

import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessTimerDome {
    /**
     * 1.keyed才会有定时器
     * 2.事件时间定时器 通过watermark来触发计算
     *      water >= 注册时间
     *      注意：watermark= 当前最大事件时间 - 等待时间- 1ms 注意这个 因为有-1ms 所以会推迟一条数据
     *          比如：5s的定时器
     *          如果 等待=3s watermark = 8s-3s - 1s = 4999ms 不会触发5s的定时器
     *          需要watermark = 9s- 3s - 1ms = 5999ms 才会触发5s的定时器
     *3.在process中获取的当前的watermark 显示的是上一次的watermark --》 因为process还没有接收到这天数据对应生成的watermark 流式计算是一条数据处理一次，watermark也是数据流，紧随其后的下一条数据就是watermark
     *
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("10.107.105.8", 7777).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.parseInt(datas[2]));
            }
        });
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.
                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(((element, recordTimestamp) -> {
                    return element.getTs() * 1000L;
                }));
        //watermark和key没有关系
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = waterSensorSingleOutputStreamOperator.keyBy(sensor -> sensor.getId());
        SingleOutputStreamOperator<String> process = sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            /**
             * 来一条数据调用一次
             * @param value The input value.
             * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
             *     {@link TimerService} for registering timers and querying the time. The context is only
             *     valid during the invocation of this method, do not store it.
             * @param out The collector for returning result values.
             * @throws Exception
             */

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                //事件中提取出来的事件时间
//                Long currentEvenTime = ctx.timestamp();
//                String currentKey = ctx.getCurrentKey();



                //定时器
                TimerService timerService = ctx.timerService();
                System.out.println("当前数据="+value+",当前的watermark"+timerService.currentWatermark());   //获取当前process的watermark
                //注册定时器 ： 事件时间
//                timerService.registerEventTimeTimer(5000L);
//                System.out.println(currentKey+"当前时间是" + currentEvenTime + ",注册了一个5s的定时器");
                //注册定时器 : 处理时间
//                long currenTS = timerService.currentProcessingTime();
//                timerService.registerProcessingTimeTimer(currenTS + 5000l);
//                System.out.println(currentKey+"当前时间是" + currentEvenTime + ",注册了一个5s后的定时器");
                //删除定时器  ： 事件时间
//                timerService.deleteEventTimeTimer();
                //删除定时器  ： 处理时间
//                timerService.deleteProcessingTimeTimer();

                //获取当前的处理时间 就是系统时间
//                long currenTS = timerService.currentProcessingTime();
                //获取当前的watermark
//                long wm = timerService.currentWatermark();
            }

            /**
             * 时间进展到定时器注册的时间 调用改方法 自动去重 对于多个key 也只会有一个定时器 也就是onTimer对于一个可以 只会被调用一次
             * @param timestamp The timestamp of the firing timer.
             * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
             *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
             *     registering timers and querying the time. The context is only valid during the invocation
             *     of this method, do not store it.
             * @param out The collector for returning result values.
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                String currentKey = ctx.getCurrentKey();
                System.out.println(currentKey+"现在的时间是:" + timestamp + "定时器触发");
            }
        });

        process.print();


        env.execute();
    }
}
