package com.puchen.java.flink117.p6sink;

import com.puchen.java.flink117.bean.WaterSensor;
import com.puchen.java.flink117.impl.WaterSensorMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;

/**
 * @ClassName: CustomSinkDome
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 18:28
 * @Version: 1.0
 **/
public class CustomSinkDome {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> master = env.socketTextStream("master", 7777).map(new WaterSensorMapFunction());

        master.addSink(new SinkFunction<WaterSensor>() {
        })

                env.execute();
    }

    public static class MySink extends RichSinkFunction<String>{

        Connection con  = null;


        /**
         * sink的核心逻辑 写出的逻辑就写在这个方法里面
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            //这个方法是来一条数据 调用一次 所以不要在这里创建连接对象
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            //在这里创建连接
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            //做一些清晰 销毁连接
            super.close();
        }
    }

}
