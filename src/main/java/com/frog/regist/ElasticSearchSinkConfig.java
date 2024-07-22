package com.frog.regist;

import org.apache.http.HttpHost;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class ElasticSearchSinkConfig implements Serializable {
    private final List<HttpHost> httpHosts;
    private final String indexName;
    private final XContentType xContentType;
    private final String username;
    private final String password;
    private final boolean auth;


    private ElasticSearchSinkConfig(Builder builder) {
        this.httpHosts = builder.httpHosts;
        this.indexName = builder.indexName;
        this.xContentType = builder.xContentType;
        this.username = builder.username;
        this.password = builder.password;
        this.auth = builder.auth;
    }

    public List<HttpHost> getHttpHosts() {
        return httpHosts;
    }

    public String getIndexName() {
        return indexName;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isAuth() {
        return auth;
    }

    public XContentType getxContentType() {
        return xContentType;
    }

    public static class Builder {
        private List<HttpHost> httpHosts = new ArrayList<>();
        private String indexName;
        private XContentType xContentType = XContentType.JSON;
        private String username;
        private String password;
        private boolean auth = false;

        public Builder withHttpHost(HttpHost httpHost) {
            this.httpHosts.add(httpHost);
            return this;
        }

        public Builder withIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder withXContentType(XContentType xContentType) {
            this.xContentType = xContentType;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withAuth(boolean auth) {
            this.auth = auth;
            return this;
        }

        public Builder withType(String type) {
            return this;
        }

        public ElasticSearchSinkConfig build() {
            return new ElasticSearchSinkConfig(this);
        }
    }
}