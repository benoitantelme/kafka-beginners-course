package kafka.tutorial2perso;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private String apiKey;
    private String apiSecretKey;
    private String accessToken;
    private String accessTokenSecret;

    public TwitterProducer() {
        Properties props;
        try {
            props = getProperties();
        } catch (IOException e) {
            LOGGER.error("Can not get properties", e);
            throw new RuntimeException(e);
        }

        this.apiKey = props.getProperty("api.key");
        this.apiSecretKey = props.getProperty("api.secret.key");
        this.accessToken = props.getProperty("access.token");
        this.accessTokenSecret = props.getProperty("access.token.secret");
    }

    private Properties getProperties() throws IOException {
        Properties prop = new Properties();
        String propFileName = "twitter.api.info.properties";

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

        return prop;
    }

    public KafkaProducer<String, String> createProducer() {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer(prop);
    }

    public Client createClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(List.of("kafka"));

        Authentication hosebirdAuth = new OAuth1(apiKey, apiSecretKey, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public static void main(String[] args) {
        TwitterProducer tp = new TwitterProducer();

        KafkaProducer<String, String> producer = tp.createProducer();

        // size the queue properly based on expected TPS of your stream
        BlockingQueue<String> mq = new LinkedBlockingQueue<>(1000);
        Client hosebirdClient = tp.createClient(mq);
        hosebirdClient.connect();

        // shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            hosebirdClient.stop();
            producer.close();
            tp.LOGGER.info("Shutdown hook done");
        }));

        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = mq.poll(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                tp.LOGGER.error("Exception while polling queue", e);
                hosebirdClient.stop();
            }

            if (msg != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>("tweets", msg);
                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        tp.LOGGER.info("Received metadata\n Topic " + recordMetadata.topic()
                                + "\n Partition " + recordMetadata.partition()
                                + "\n Offset " + recordMetadata.offset()
                                + "\n Timestamp " + recordMetadata.timestamp());
                    } else {
                        tp.LOGGER.error("Exception while sending record", e);
                    }
                });
            }
        }
        tp.LOGGER.info("End of processing.");
    }

}
