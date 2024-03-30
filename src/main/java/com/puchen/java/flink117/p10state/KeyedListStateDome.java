package com.puchen.java.flink117.p10state;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 检测每种传感器的水位，，如果连续的两个水位值超过10 就输出报警
 *
 * @ClassName: KeyedValueStateDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/29 20:57
 * @Version: 1.0
 **/
public class KeyedListStateDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("slave2", 7777).map(new WaterSensorMapFunction()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(((element, ts) -> element.getTs() * 1000L)));
        sensorDS.keyBy(r -> r.getId()).process(new KeyedProcessFunction<String, WaterSensor, String>() {
            ListState<Integer> vcListState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                vcListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcListState", Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                //来一条到list里面
                vcListState.add(value.getVc());
                //从list状态拿出来 copy到一个list中排序 取前3最大的

                Iterable<Integer> vcListIt = vcListState.get();
                List<Integer> vcList = new ArrayList<>();
                for (Integer vc : vcListIt) {
                    vcList.add(vc);
                }
                vcList.sort((o1, o2) -> o2-o1);

                //只保留最大的三个  list中的个数一定是连续变大 只要超过3就立即清理
                if (vcList.size()>3) {
                    //将第四个数据清除
                    vcList.remove(3);
                }
                out.collect("传感器id为"+value.getId()+",最大的3个水位值="+vcList.toString());
                //更新list
                vcListState.update(vcList);

/**
                vcListState.get();  //取出list状态的数据是一个iterable
                vcListState.add() ; //向list状态添加一个元素
                vcListState.addAll(); ; //向list状态添加多个元素
                vcListState.update(); ; //更新list状态 覆盖
                vcListState.clear(); ; //清空list状态
*/

            }
        }).print();

    env.execute();
    }

}
