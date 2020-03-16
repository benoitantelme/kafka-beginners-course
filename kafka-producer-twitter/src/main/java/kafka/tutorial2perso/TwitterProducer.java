package kafka.tutorial2perso;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TwitterProducer {
    Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String apiKey;
    String apiSecretKey;
    String accessToken;
    String accessTokenSecret;

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

    public KafkaProducer<String, String> createProducer(){
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer(prop);
    }

    public static void main(String[] args){
        TwitterProducer tp = new TwitterProducer();

        System.out.println("a");

    }


}
