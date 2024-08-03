package com.frog.regist;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.common.xcontent.XContentType;

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
public class regist_with_idfdt_fix01 {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 1. 读取业务主流
        String topic1 = "gateway_data";
        String groupId1 = "gateway_data_0802";
        String topic2 = "kochava";
        String groupId2 = "kochava_0802";
        //DataStreamSource<String> ds_gateway = env.addSource(KafkaUtil.getKafkaConsumer(topic1, groupId1));
        //DataStreamSource<String> ds_kochava = env.addSource(KafkaUtil.getKafkaConsumer(topic2, groupId2));
        DataStreamSource<String> ds_gateway = env.readTextFile("D:\\code\\user_wallet_permission\\input\\gateway.txt");
        DataStreamSource<String> ds_kochava = env.readTextFile("D:\\code\\user_wallet_permission\\input\\kochava_original_data.txt");
        //todo 2.数据结构转化

        SingleOutputStreamOperator<JSONObject> json_gateway = ds_gateway.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> json_kochava = ds_kochava.map(JSON::parseObject);
        //creative_id,
        //ef_idfdt
        //todo 3.点击广告数据为主流 对主流数据ETL.留下指定的广告数据  国家过滤 主键过滤ef_idfdt,且只有create_id不为空的才能往下发
        SingleOutputStreamOperator<JSONObject> filte_kochava = json_kochava.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {

                try {
                    if (jsonObject.getString("ef_idfdt") != "" && jsonObject.getString("creative_id").length()>1) {
                        if ((jsonObject.getString("country").equals("GB") || jsonObject.getString("country").equals("US"))) {
                           /* String creative_id = jsonObject.getString("creative_id");
                            int length = creative_id.length();
                            String idfdt = jsonObject.getString("ef_idfdt");
                            System.out.println("主流数据过滤：  " + idfdt  + "<-------------> " + creative_id + "广告id长度为： " + length);*/
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return false;

                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        //todo 4.注册数据为次流 ，对次流ETL 留下指定的接口  主键过滤idfdt
        SingleOutputStreamOperator<JSONObject> filte_gateway = json_gateway.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                if (jsonObject != null && jsonObject.getString("responseData")!= null  ) {
                    JSONObject responseData = jsonObject.getJSONObject("responseData");
                    if (!responseData.isEmpty()) {
                        if (responseData.getString("data")!= null) {
                            JSONObject data = responseData.getJSONObject("data");
                            if (!data.isEmpty()) {
                                if (data.getString("dataObject")!= null) {
                                    JSONObject dataObject = data.getJSONObject("dataObject");
                                    if (!dataObject.isEmpty()) {
                                        String requestPath = jsonObject.getString("requestPath"); // 确保jsonObject非空且包含"requestPath" 这是最外层结构
                                        Long id = dataObject.getLong("id");
                                        String idfdt = dataObject.getString("idfdt");

                                        if (requestPath != null && (
                                                requestPath.contains("/growAlong/v1/api/common/validSmsCode") ||
                                                        requestPath.contains("/growAlong/v1/api/common/verifyThirdAccount") ||
                                                        requestPath.contains("/growAlong/v1/api/common/sendSmsV3")
                                        ) && id >0
                                        && idfdt !=""
                                        ) {
                                            return true;
                                        } else {
                                            return false;
                                        }
                                    } else {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }

            }
        });

        //todo 5.主流map 结构转换 <creative_id,ef_idfdt,country,ts>
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

        //todo 6.次流map 结构转换 <user_id,idfdt,ts>
        //gateway数据  最外层 get_json_object(data,'$.responseData') as responseData
        //gateway数据  get_json_object(get_json_object(a.responseData,'$.data'),'$.dataObject') as dataObject  --->get_json_object(dataObject,'$.idfdt') as idfdt
        //gateway数据  get_json_object(get_json_object(a.responseData,'$.data'),'$.dataObject') as dataObject  --->get_json_object(dataObject,'$.id') as user_id
        SingleOutputStreamOperator<Tuple3<Long, String, Long>> map_gateway = filte_gateway.map(new MapFunction<JSONObject, Tuple3<Long, String, Long>>() {
            @Override
            public Tuple3<Long, String, Long> map(JSONObject jsonObject) throws Exception {
                return Tuple3.of(
                        jsonObject.getJSONObject("responseData").getJSONObject("data").getJSONObject("dataObject").getLong("id"),
                        jsonObject.getJSONObject("responseData").getJSONObject("data").getJSONObject("dataObject").getString("idfdt"),
                        //Calendar.getInstance().getTimeInMillis());
                        jsonObject.getLong("responseTime"));
            }
        });

  //todo 7 双流connect  <user_id,idfdt,ts>  &  <creative_id,ef_idfdt,country,ts>
   ConnectedStreams<Tuple4<String, String, String, Long>, Tuple3<Long, String, Long>> connect = map_kochava.connect(map_gateway);
        ConnectedStreams<Tuple4<String, String, String, Long>, Tuple3<Long, String, Long>> ConnectedStreams = connect.keyBy(s1 -> s1.f1, s2 -> s2.f1);
        //todo 8 process 合并上述结构   <user_id,creative_id,country,ts> 需要使用定时器进行触发
        SingleOutputStreamOperator<Tuple4<Long, String, String, Long>> process = ConnectedStreams.process(new CoProcessFunction<Tuple4<String, String, String, Long>, Tuple3<Long, String, Long>, Tuple4<Long, String, String, Long>>() {
            //todo 实现两条流互相join的效果 由于哪条流数据先来是不确定的，因此引入一个用来临时存储数据的媒介，当一条流数据到了，先放入媒介，然后去另外一条流的数据查看数据是否来了，若来了，则与其进行匹配
            // 声明状态变量用来保存 kochava事件
            private ValueState<Tuple4<String, String, String, Long>> kochava_Event; //  <creative_id,ef_idfdt,country,ts>
            // 声明状态变量用来保存gateway_Event的事件
            private ValueState<Tuple3<Long, String, Long>> gateway_Event; // <user_id,idfdt,ts>
            //collect 收集数据的目标结构：  <user_id,creative_id,country,ts> ts使用注册app时间

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                kochava_Event = getRuntimeContext().getState(
                        new ValueStateDescriptor<Tuple4<String, String, String, Long>>("kochava", Types.TUPLE(
                                Types.STRING, Types.STRING, Types.STRING, Types.LONG
                        ))
                );

                gateway_Event = getRuntimeContext().getState(
                        new ValueStateDescriptor<Tuple3<Long, String, Long>>("geteway", Types.TUPLE(
                                Types.LONG, Types.STRING, Types.LONG
                        ))
                );
            }

            @Override   //站在主流看侧流数据是否到来  ,collect 收集数据的目标结构：  <user_id,creative_id,country,ts> ts使用注册app时间
            public void processElement1(Tuple4<String, String, String, Long> kochava, CoProcessFunction<Tuple4<String, String, String, Long>, Tuple3<Long, String, Long>, Tuple4<Long, String, String, Long>>.Context ctx, Collector<Tuple4<Long, String, String, Long>> out) throws Exception {
                if (gateway_Event.value() != null) {
                    out.collect(Tuple4.of(gateway_Event.value().f0, kochava.f0, kochava.f2, gateway_Event.value().f2));
                    gateway_Event.clear();
                } else {
                    kochava_Event.update(kochava);
                    ctx.timerService().registerEventTimeTimer(kochava.f3 + 1000 * 60 * 20L);//默认都是毫秒，此处等待20min
                }
            }

            @Override
            public void processElement2(Tuple3<Long, String, Long> gateway, CoProcessFunction<Tuple4<String, String, String, Long>, Tuple3<Long, String, Long>, Tuple4<Long, String, String, Long>>.Context ctx, Collector<Tuple4<Long, String, String, Long>> out) throws Exception {
                if (kochava_Event.value() != null) {
                    out.collect(Tuple4.of(gateway.f0, kochava_Event.value().f0, kochava_Event.value().f2, gateway.f2));
                    kochava_Event.clear();
                } else {
                    gateway_Event.update(gateway);
                    ctx.timerService().registerEventTimeTimer(gateway.f2 + 1000 * 60 * 20L);
                }
            }
            @Override //触发定时器 说明数据超时了 已经失效
            public void onTimer(long timestamp, CoProcessFunction<Tuple4<String, String, String, Long>, Tuple3<Long, String, Long>, Tuple4<Long, String, String, Long>>.OnTimerContext ctx, Collector<Tuple4<Long, String, String, Long>> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                if (gateway_Event.value() != null) {
                    gateway_Event.clear();
                }
                if (kochava_Event.value() != null) {
                    kochava_Event.clear();
                }
            }
        });
        SingleOutputStreamOperator<Tuple4<Long, String, String, Long>> filtercreateid = process.filter(new FilterFunction<Tuple4<Long, String, String, Long>>() {
            @Override
            public boolean filter(Tuple4<Long, String, String, Long> outvalue) throws Exception {
                if (outvalue.f1 != null) {
                    return true;
                } else {

                    return false;
                }
            }
        });

        SingleOutputStreamOperator<String> mapstream = filtercreateid.map(new MapFunction<Tuple4<Long, String, String, Long>, String>() {
            @Override
            public String map(Tuple4<Long, String, String, Long> longStringStringLongTuple4) throws Exception {
                //  <user_id,creative_id,country,ts>
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("user_id",longStringStringLongTuple4.f0);
                jsonObject.put("creative_id",longStringStringLongTuple4.f1);
                jsonObject.put("country",longStringStringLongTuple4.f2);
                jsonObject.put("ts",longStringStringLongTuple4.f3);

                return jsonObject.toString();
            }
        });


        HttpHost httpHosts = new HttpHost("vpc-prod-opensearch-e6zsd5jz7y24fgwlhws6gwofzm.us-east-1.es.amazonaws.com", 443, "https");
        XContentType xContentType = XContentType.JSON;
        boolean auth = true; // 假设需要认
        // 使用ElasticSearchSinkConfig.Builder来构建ElasticSearchSinkConfig实例
        ElasticSearchSinkConfig config = new ElasticSearchSinkConfig.Builder()
                .withHttpHost(httpHosts)
                .withIndexName("regist_kochava")
                .withXContentType(xContentType)
                .withUsername("prod-opensearch-syncDB-user")
                .withPassword("sFZNHvVq$vHpg8H9")
                .withAuth(auth)
                .build();
        OpenSearchSinkFactory openSearchSinkFactory = new OpenSearchSinkFactory();
        ElasticsearchSink<String> openSearchSink = openSearchSinkFactory.createOpenSearchSink(config);
        mapstream.addSink(openSearchSink);
        env.execute();
    }
}
