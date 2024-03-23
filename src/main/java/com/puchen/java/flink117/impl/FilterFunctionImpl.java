package com.puchen.java.flink117.impl;

import com.puchen.java.flink117.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @ClassName: FilterFunctionImpl
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 10:04
 * @Version: 1.0
 **/
public class FilterFunctionImpl implements FilterFunction<WaterSensor> {
    public String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.id.equals(value.getId());
    }
}
