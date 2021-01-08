package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by lizhenchao on 2020/12/29.
 */
public class ESClient {

    private static final Logger log = LoggerFactory.getLogger(ESClient.class);

    private RestClient restClient;

    public RestClient getClient() {
        return restClient;
    }

    public void createClient(String endpoint,
                             String username,
                             String password) {

        List<HttpHost> hostsList = new ArrayList<>();
        String[] nodes = endpoint.split(",");
        if(null != nodes && nodes.length > 0){
            for (String node : nodes) {
                String[] arr = node.split(":");
                if(arr.length == 2){
                    hostsList.add(new HttpHost(arr[0], Integer.parseInt(arr[1])));
                }
            }
        }
        if(hostsList.size() > 0) {
            HttpHost[] hosts = new HttpHost[hostsList.size()];
            hostsList.toArray(hosts);

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            final RequestConfig reqConf = RequestConfig.custom().setConnectTimeout(5000)
                    .setSocketTimeout(30 * 60 * 1000)
                    .setConnectionRequestTimeout(30 * 1000).build();

            restClient = RestClient.builder(hosts).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    httpClientBuilder.setDefaultRequestConfig(reqConf);
                    httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(1).build());
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            }).setMaxRetryTimeoutMillis(30 * 60 * 1000).build();
        }
    }

    public boolean indicesExists(String indexName) throws Exception {
        JSONObject result = sendRequestToOrigin("GET", indexName, "");
        JSONObject j = result.getJSONObject(indexName);
        if (null == j) {
            return false;
        } else {
            return true;
        }
    }

    public boolean deleteIndex(String indexName) throws Exception {
        log.info("delete index " + indexName);
        if (indicesExists(indexName)) {
            JSONObject result = sendRequestToOrigin("DELETE", indexName, "");
            if (result.getBooleanValue("acknowledged")) {
                return true;
            } else {
                return false;
            }
        } else {
            log.info("index cannot found, skip delete " + indexName);
            return true;
        }
    }

    public JSONObject sendRequestToOrigin(String method, String endpoint, String requestBody) {
        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        Request request = new Request(method, endpoint);
        request.setEntity(entity);
        try {
            Response response = restClient.performRequest(request);
            return JSON.parseObject(EntityUtils.toString(response.getEntity()));
        } catch (ResponseException e) {
            HttpEntity en = ((ResponseException) e).getResponse().getEntity();
            try {
                return JSON.parseObject(EntityUtils.toString(en));
            } catch (IOException ex) {
                ex.printStackTrace();
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public JSONObject sendRequestToOrigin2(String method, String endpoint, String requestBody) throws IOException {
        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        Request request = new Request(method, endpoint);
        request.setEntity(entity);
        try {
            Response response = restClient.performRequest(request);
            return JSON.parseObject(EntityUtils.toString(response.getEntity()));
        } catch (ResponseException e) {
            HttpEntity en = ((ResponseException) e).getResponse().getEntity();
            return JSON.parseObject(EntityUtils.toString(en));
        }
    }

    public boolean createIndex(String indexName, JSONObject settings, JSONObject mappings) throws Exception {
        if (!indicesExists(indexName)) {
            JSONObject json = new JSONObject();
            json.put("settings", settings);
            json.put("mappings", mappings);
            JSONObject result = sendRequestToOrigin("PUT", indexName, json.toJSONString());
            if (result.getBooleanValue("acknowledged")) {
                log.info(String.format("create [%s] index success", indexName));
                return true;
            } else {
                log.error(String.format("create [%s] index fail", indexName));
                return false;
            }
        } else {
            log.info(String.format("index [%s] already exists", indexName));
            return true;
        }
    }

    public Set getAlias(String indexName){
        JSONObject result = sendRequestToOrigin("GET", indexName+"/_alias", "");
        JSONObject aliases = result.getJSONObject(indexName).getJSONObject("aliases");
        return aliases.keySet();
    }

    public boolean alias(String indexName, String aliasName, boolean needClean) throws IOException {
        if(needClean){
            Set<String> aliases = getAlias(indexName);
            if(aliases.size() > 0){
                for (String alias : aliases) {
                    String requestBody = "{\"actions\":[{\"remove\":{\"index\":\""+indexName+"\",\"alias\":\""+alias+"\"}}]}";
                    JSONObject result = sendRequestToOrigin("POST", "/_aliases", requestBody);
                    if(result.getBooleanValue("acknowledged")){
                        log.info("index {} remove alias {} success", indexName, alias);
                    }else {
                        log.warn("index {} remove alias {} fail", indexName, alias);
                    }
                }
            }
            String requestBody = "{\"actions\":[{\"add\":{\"index\":\""+indexName+"\",\"alias\":\""+aliasName+"\"}}]}";
            JSONObject result = sendRequestToOrigin("POST", "/_aliases", requestBody);
            return result.getBooleanValue("acknowledged");
        }else{
            String requestBody = "{\"actions\":[{\"add\":{\"index\":\""+indexName+"\",\"alias\":\""+aliasName+"\"}}]}";
            JSONObject result = sendRequestToOrigin("POST", "/_aliases", requestBody);
            return result.getBooleanValue("acknowledged");
        }
    }

    /**
     * 关闭RestClient客户端
     *
     */
    public void closeRestClient() {
        if (restClient != null) {
            try {
                restClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
