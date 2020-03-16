package kafka.tutorial1perso;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoGroups {
    private String bootstrapServer = "localhost:9092";
    private String groupId = "third_app";
    private String topic = "first_topic";


    private static Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    public static void main(String[] args) {
        ConsumerDemoGroups consumerDemoGroups = new ConsumerDemoGroups();

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerDemoGroups.bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerDemoGroups.groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        consumer.subscribe(List.of(consumerDemoGroups.topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                LOGGER.info("Received record with key : " + record.key()
                        + "\nValue " + record.value()
                        + "\nPartition " + record.partition()
                        + "\nOffset " + record.offset());
        }

    }

}
