package com.puchen.java.flink117.FunctionUtil;

import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class FunctionUtilImpl implements MapFunction<WaterSensor,String>{
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
