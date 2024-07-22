package com.frog.regist;

/**
 * packageName com.commercial
 *
 * @author yanxuechao
 * @version JDK 8
 * @className OpenSearchSinkFactory
 * @date 2024/7/9
 * @description TODO
 */

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.Serializable;

public class OpenSearchSinkFactory implements Serializable {
    public static ElasticsearchSink<Tuple4<Long, String, String, Long>> createOpenSearchSink(ElasticSearchSinkConfig elasticSearchSinkConfig) {
        ElasticsearchSink.Builder<Tuple4<Long, String, String, Long>> elasticSearchBuilder = new ElasticsearchSink.Builder<>(elasticSearchSinkConfig.getHttpHosts()
                , new ElasticsearchSinkFunction<Tuple4<Long, String, String, Long>>() {
            public IndexRequest createIndexRequest(Tuple4<Long, String, String, Long> element) {
                return Requests.indexRequest()
                        .index(elasticSearchSinkConfig.getIndexName())
                        .source(element, elasticSearchSinkConfig.getxContentType());
            }

            @Override
            public void process(Tuple4<Long, String, String, Long> element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(createIndexRequest(element));
            }
        });
        if (elasticSearchSinkConfig.isAuth()) {
            elasticSearchBuilder.setRestClientFactory(
                    (RestClientFactory) restClientBuilder -> restClientBuilder.setHttpClientConfigCallback(
                            httpAsyncClientBuilder -> {
                                BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
                                basicCredentialsProvider.setCredentials
                                        (
                                                AuthScope.ANY,
                                                new UsernamePasswordCredentials
                                                        (
                                                                elasticSearchSinkConfig.getUsername(),
                                                                elasticSearchSinkConfig.getPassword()
                                                        )
                                        );
                                httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
                                return httpAsyncClientBuilder;
                            })
            );

        }
        return elasticSearchBuilder.build();
    }

}

