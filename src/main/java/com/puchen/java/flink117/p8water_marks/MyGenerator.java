package com.puchen.java.flink117.p8water_marks;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @ClassName: MyGenerator
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/24 20:17
 * @Version: 1.0
 **/
public class MyGenerator<T> implements WatermarkGenerator<T> {

    //乱序的等待时间
    private long delayTs;
    //用来保存当前位置 最大的时间时间
    private long maxTs;

    public MyGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每个数据来都会调用一次 用来提起最的时间时间  保存下来
     *
     * @param event
     * @param eventTimestamp 提取到的数据的事件事件
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        System.out.println("调用了onEvent方法，获取目前为止的最大时间错="+ maxTs);
        maxTs = Math.max(maxTs, eventTimestamp);
    }

    /**
     * 周期性调用 watermark
     *
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用了onPeriodicEmit方法，生成water=" + (maxTs - delayTs - 1));
    }
}
