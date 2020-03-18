package com.github.simplesteph.kafka.tutorial3perso;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ElasticSearchConsumer {
    static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private JsonParser jsonParser = new JsonParser();
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

    public RestHighLevelClient createClient() {
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

    public KafkaConsumer<String, String> createConsumer(String topic) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka_demo_elasticsearch");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        consumer.subscribe(List.of(topic));

        return consumer;
    }

    private String extractIdFrom(String jsoncontent){
        return jsonParser.parse(jsoncontent).getAsJsonObject().get("id_str").getAsString();
    }

    public static void main(String[] args) throws IOException {
        ElasticSearchConsumer esc = new ElasticSearchConsumer();
        RestHighLevelClient client = esc.createClient();
        KafkaConsumer<String, String> consumer = esc.createConsumer("tweets");

        // shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOGGER.info("Shutdown hook done");
        }));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String id = esc.extractIdFrom(record.value());
                IndexRequest request = new IndexRequest("twitter")
                        .source(record.value(), XContentType.JSON)
                        .id(id); // this is to make our consumer idempotent

                IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                LOGGER.info(response.getId());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
