package com.puchen.java.flink117.impl;

import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @ClassName: WaterSensorMapFunction
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 14:34
 * @Version: 1.0
 **/
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        return new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2]));
    }
}
