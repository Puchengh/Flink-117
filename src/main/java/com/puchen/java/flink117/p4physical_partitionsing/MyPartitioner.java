package com.puchen.java.flink117.p4physical_partitionsing;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @ClassName: MyPartitioner
 * @Desc: TODO
 * @Author: puchen
 * @Date: 2024/3/23 11:02
 * @Version: 1.0
 **/
public class MyPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key)%numPartitions;
    }
}
