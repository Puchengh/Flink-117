package com.puchen.java.flink117.p10state;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 检测每种传感器的水位，，如果连续的两个水位值超过10 就输出报警
 *
 * @ClassName: KeyedValueStateDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/29 20:57
 * @Version: 1.0
 **/
public class KeyedValueStateDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(((element, ts) -> element.getTs() * 1000L)));
        sensorDS.keyBy(r -> r.getId()).process(new KeyedProcessFunction<String, WaterSensor, String>() {
            ValueState<Integer> lastVcState;

//            int lastVc;  //不能使用普通变量 不会按照分类,分组
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                /**
                 * 状态描述器 第一个参数 名字 唯一不重复  第二个参数 存储的类型
                 */
                lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                // 1取出上一条的水位值
//                lastVcState.value(); //取出值状态里面的数据
//                lastVcState.update();  //更新值状态里面的数据
//                lastVcState.clear();  //清楚值状态里面的数据
                /**
                 * 更新窗台的水位值
                 */
                Integer vc = value.getVc();
                int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();

                //2求差值的绝对值 判断是否超过20
                if (Math.abs(vc-lastVc) > 10) {
                    out.collect("传感器="+value.getId()+"当前的水位值="+vc+",与上一条水位值"+lastVc+",相差超过10！！！！");
                }
                //3更新状态的水位值

                lastVcState.update(vc);

            }
        }).print();

    env.execute();
    }

}
