package com.puchen.java.flink117.p9process;

import java.time.Duration;


import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutPutDome {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("10.107.105.8", 7777).map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.parseInt(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                                                       <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                                       .withTimestampAssigner(((element, recordTimestamp) -> {
                                                           return element.getTs() * 1000L;
                                                       })));
        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.keyBy(sensor -> sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        //使用测输出流告警
                        if (value.getVc() > 10) {
                            ctx.output(warnTag, "当前水位" + value.getVc() + ",大于阈值10!!!");
                        }
                        //主流正常发送数据
                        out.collect(value);
                    }
                });
        process.print("主流");
        process.getSideOutput(warnTag).printToErr("warn");

        env.execute();
    }
}
