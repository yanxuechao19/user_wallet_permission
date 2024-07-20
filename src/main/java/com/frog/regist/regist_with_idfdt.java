package com.frog.regist;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.frog.source.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;


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
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

         /*       if(jsonObject.getJSONObject("responseData").getJSONObject("data").has){


                }*/




                try {
                    if (    (jsonObject.getString("requestPath").contains("/growAlong/v1/api/common/validSmsCode")
                            || jsonObject.getString("requestPath").contains("/growAlong/v1/api/common/verifyThirdAccount")
                            || jsonObject.getString("requestPath").contains("/growAlong/v1/api/common/sendSmsV3"))
/*                            && jsonObject.getString("requestPath").length()>0*/
                            /*&& jsonObject.getJSONObject("responseData").getJSONObject("data").getJSONObject("dataObject").length()>0*/
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
/*                    if (    jsonObject.getString("country ").equals("GB") || jsonObject.getString("country ").equals("US")
                            &&
                            (jsonObject.getString("creative_id").equals("1804620165127170") || jsonObject.getString("creative_id").equals("1804620165132338") || jsonObject.getString("creative_id").equals("1804620298791937") || jsonObject.getString("creative_id").equals("1804620298796081") )
                    )*/

                        if (jsonObject.getString("country").equals("GB")) {
                        return true;} else {
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
                return Tuple3.of(
                        jsonObject.getJSONObject("responseData").getJSONObject("data").getJSONObject("dataObject").getString("id"),
                        jsonObject.getJSONObject("responseData").getJSONObject("data").getJSONObject("dataObject").getString("idfdt"),
                        //Calendar.getInstance().getTimeInMillis());
                        jsonObject.getLong("responseTime"));
            }
        });

        //todo 6.次流map 结构转换 <creative_id,ef_idfdt,country,ts>
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> map_kochava = filte_kochava.map(new MapFunction<JSONObject, Tuple4<String, String, String, Long>>() {

            @Override
            public Tuple4<String, String, String, Long> map(JSONObject jsonObject) throws Exception {
                return Tuple4.of(
                        jsonObject.getString("creative_id"),
                        jsonObject.getString("ef_idfdt"),
                        jsonObject.getString("country"),
                        jsonObject.getLong("event_timestamp"));
                        //Calendar.getInstance().getTimeInMillis());
            }
        });
        //todo 7 双流connect  <user_id,idfdt,ts>  &  <creative_id,ef_idfdt,country,ts>
        ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> connect = map_gateway.connect(map_kochava);
        ConnectedStreams<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>> ConnectedStreams = connect.keyBy(s1 -> s1.f1, s2 -> s2.f1);

        //todo 8 process 合并上述结构   <user_id,creative_id,country,ts> 需要使用定时器进行触发
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> process = ConnectedStreams.process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>>() {
            //todo 实现两条流互相join的效果 由于哪条流数据先来是不确定的，因此引入一个用来临时存储数据的媒介，当一条流数据到了，先放入媒介，然后去另外一条流的数据查看数据是否来了，若来了，则与其进行匹配

            // 声明状态变量用来保存app的事件
            private ValueState<Tuple3<String, String, Long>> gateway_Event; // <user_id,idfdt,ts>
            // 声明状态变量用来保存第三方平台事件
            private ValueState<Tuple4<String, String, String, Long>> kochava_Event; //  <creative_id,ef_idfdt,country,ts>

            //collect 收集数据的目标结构：  <user_id,creative_id,country,ts> ts使用注册app时间
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                gateway_Event = getRuntimeContext().getState(
                        new ValueStateDescriptor<Tuple3<String, String, Long>>("geteway", Types.TUPLE(
                                Types.STRING, Types.STRING, Types.LONG
                        ))
                );


                kochava_Event = getRuntimeContext().getState(
                        new ValueStateDescriptor<Tuple4<String, String, String, Long>>("kochava", Types.TUPLE(
                                Types.STRING, Types.STRING, Types.STRING, Types.LONG
                        ))
                );


            }

            @Override
            public void processElement1(Tuple3<String, String, Long> gateway, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>>.Context context, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {

                // TODO 1.来过的s1数据，都存起来
                if (kochava_Event.value() != null) {
                    collector.collect(Tuple4.of(gateway.f0, kochava_Event.value().f0, kochava_Event.value().f2, kochava_Event.value().f3));
                } else {
                    gateway_Event.update(gateway);
                    context.timerService().registerEventTimeTimer(gateway.f2 + 1000 * 60 * 20L);//默认都是毫秒，此处等待20min
                }
            }

            // <creative_id,ef_idfdt,country,ts>
            // <user_id,idfdt,ts>
            @Override
            public void processElement2(Tuple4<String, String, String, Long> kochava, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>>.Context context, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {
                if (gateway_Event.value() != null) {
                    collector.collect(Tuple4.of(gateway_Event.value().f0, kochava.f0, kochava.f2, gateway_Event.value().f2));
                    gateway_Event.clear();
                } else {
                    kochava_Event.update(kochava);
                    context.timerService().registerEventTimeTimer(kochava.f3 + 1000 * 60 * 20L);//默认都是毫秒，此处等待20min
                }
            }

            @Override
            public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>>.OnTimerContext ctx, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
                super.onTimer(timestamp, ctx, out);

                if(gateway_Event.value() !=null){
                  //  out.collect(Tuple4.of(gateway_Event.value().f0, gateway_Event.value().f1,gateway_Event.value().f2,99999999999999l ));
                    out.collect(Tuple4.of(gateway_Event.value().f0,gateway_Event.value().f1,"false",gateway_Event.value().f2));
                    gateway_Event.clear();
                }

                if(kochava_Event.value() !=null){
                    //  out.collect(Tuple4.of(gateway_Event.value().f0, gateway_Event.value().f1,gateway_Event.value().f2,99999999999999l ));
                    out.collect(Tuple4.of(kochava_Event.value().f0,kochava_Event.value().f1,"false",kochava_Event.value().f3));
                    kochava_Event.clear();
                }
            }
        });
        process.print();
        env.execute();



    }
}
