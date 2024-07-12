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
        String topic = "gateway_data";
        String groupId = "gateway_data_0712";
        DataStreamSource<String> coinDs = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
        //DataStreamSource<String> coinDs = env.readTextFile("D:\\code\\commercial_on_ds\\input\\user_coin_log.txt");



    }
}
