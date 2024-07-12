package com.frog.regist;

import com.frog.source.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * packageName com.frog.regist
 *
 * @author yanxuechao
 * @version JDK 8
 * @className regist_with_idfdt
 * @date 2024/7/12
 * @description TODO
 */
public class regist_with_idfdt {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        // TODO 1. 读取业务主流
        String topic = "growalong_prod";
        String groupId = "growalong_prod_0702";
        DataStreamSource<String> coinDs = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));


    }
}
