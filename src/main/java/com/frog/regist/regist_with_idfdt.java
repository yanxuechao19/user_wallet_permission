package com.frog.regist;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.frog.source.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.sql.Timestamp;
import java.util.Calendar;


/**
 * packageName com.frog.regist
 *
 * @author yanxuechao
 * @version JDK 8
 * @className regist_with_idfdt
 * @date 2024/7/12
 * @description TODO
 */
//验证接口数据 用于获取注册用户id
//用户国家数据 用于决定向哪个地区投放的时间 可以直接从广告中的country获取 GB 英国  US 美国
//广告数据    只对指定的广告渠道进来的用户进行推文投放
public class regist_with_idfdt {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        // TODO 1. 读取业务主流
        String topic1 = "gateway_data";
        String groupId1 = "gateway_data_0712";
        String topic2 = "kochava";
        String groupId2 = "kochava_0712";
        //DataStreamSource<String> coinDs = env.addSource(KafkaUtil.getKafkaConsumer(topic1, groupId1));
        //DataStreamSource<String> coinDs = env.addSource(KafkaUtil.getKafkaConsumer(topic2, groupId2));
        DataStreamSource<String> ds_gateway = env.readTextFile("D:\\code\\user_wallet_permission\\input\\gateway.txt");
        DataStreamSource<String> ds_kochava = env.readTextFile("D:\\code\\user_wallet_permission\\input\\kochava_original_data.txt");
        //todo 2.数据结构转化
        SingleOutputStreamOperator<JSONObject> json_gateway = ds_gateway.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> json_kochava = ds_kochava.map(JSON::parseObject);
        //creative_id,
        //ef_idfdt
        //todo 3.主流ETL 留下指定的接口
        SingleOutputStreamOperator<JSONObject> filte_gateway = json_gateway.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //get_json_object(data,'$.requestPath') LIKE '%/growAlong/v1/api/common/validSmsCode%'
                // /growAlong/v1/api/common/verifyThirdAccount
                // /growAlong/v1/api/common/sendSmsV3
                try {
                    if (    (jsonObject.getString("requestPath").contains("/growAlong/v1/api/common/validSmsCode")
                            || jsonObject.getString("requestPath").contains("/growAlong/v1/api/common/verifyThirdAccount")
                            || jsonObject.getString("requestPath").contains("/growAlong/v1/api/common/sendSmsV3"))
                            && jsonObject.getString("requestPath").length()>0
                            && jsonObject.getJSONObject("responseData").getJSONObject("data").getJSONObject("dataObject") != null
                    ) {
                        return true;
                    } else {

                        return false;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        //todo 4.次流数据ETL.留下指定的广告数据
        SingleOutputStreamOperator<JSONObject> filte_kochava = json_kochava.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {

                try {
                    if (    (jsonObject.getString("country ").equals("GB") || jsonObject.getString("country ").equals("US"))
                            &&
                            (jsonObject.getString("creative_id").equals("1804620165127170") || jsonObject.getString("creative_id").equals("1804620165132338") || jsonObject.getString("creative_id").equals("1804620298791937") || jsonObject.getString("creative_id").equals("1804620298796081") )
                    ) {
                        return true;
                    } else {

                        return false;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        });
        //todo 5.主流map 结构转换 <user_id,idfdt,ts>
        //gateway数据  最外层 get_json_object(data,'$.responseData') as responseData
        //gateway数据  get_json_object(get_json_object(a.responseData,'$.data'),'$.dataObject') as dataObject  --->get_json_object(dataObject,'$.idfdt') as idfdt
        //gateway数据  get_json_object(get_json_object(a.responseData,'$.data'),'$.dataObject') as dataObject  --->get_json_object(dataObject,'$.id') as user_id
        SingleOutputStreamOperator<Tuple3<String, String, Long>> map_gateway = filte_gateway.map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(JSONObject jsonObject) throws Exception {
                return Tuple3.of(jsonObject.getJSONObject("responseData").getJSONObject("data").getJSONObject("dataObject").getString("id"), jsonObject.getJSONObject("responseData").getJSONObject("data").getJSONObject("dataObject").getString("idfdt"), Calendar.getInstance().getTimeInMillis());
            }
        });

        //todo 6.次流map 结构转换 <creative_id,ef_idfdt,country,ts>
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> map_kochava = filte_kochava.map(new MapFunction<JSONObject, Tuple4<String, String, String, Long>>() {

            @Override
            public Tuple4<String, String, String, Long> map(JSONObject jsonObject) throws Exception {
                return Tuple4.of(jsonObject.getString("creative_id"), jsonObject.getString("ef_idfdt"), jsonObject.getString("country"), Calendar.getInstance().getTimeInMillis());
            }
        });
        //todo 7 双流connect  <user_id,idfdt,ts>  &  <creative_id,ef_idfdt,country,ts>
        ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> connect = map_gateway.connect(map_kochava);
        ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> tuple3Tuple4ConnectedStreams = connect.keyBy(s1 -> s1.f1, s2 -> s2.f1);
        //todo 8 再次map 合并上述结构   <user_id,creative_id,country,ts>
        //todo 未写完！！！！！！！！！！！！！！！！！！！
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> map = tuple3Tuple4ConnectedStreams.map(new CoMapFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>>() {
            @Override
            public Tuple4<String, String, String, Long> map1(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                return null;
                //todo 未写完！！！！！！！！！！！！！！！！！！！
            }

            @Override
            public Tuple4<String, String, String, Long> map2(Tuple4<String, String, String, Long> stringStringStringLongTuple4) throws Exception {
                return null;
                //todo 未写完！！！！！！！！！！！！！！！！！！！
            }
        });


    }
}
