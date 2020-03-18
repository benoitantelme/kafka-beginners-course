package com.github.simplesteph.kafka.tutorial3perso;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ElasticSearchConsumer {
    static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private String hostname;
    private String username;
    private String password;

    public ElasticSearchConsumer() throws IOException {
        Properties prop = new Properties();
        String propFileName = "elasticsearch.bonsai.info.properties";

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

        this.hostname = prop.getProperty("hostname");
        this.username = prop.getProperty("username");
        this.password = prop.getProperty("password");
    }

    public RestHighLevelClient createClient(){
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        ElasticSearchConsumer esc = new ElasticSearchConsumer();
        RestHighLevelClient client = esc.createClient();

        String jsonstring = "{ \" foo \": \" bar \" }";

        IndexRequest request = new IndexRequest("twitter", "tweets")
                .source(jsonstring, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String id = response.getId();
        LOGGER.info(id);


        client.close();
    }

}
