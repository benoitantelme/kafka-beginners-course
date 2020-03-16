package kafka.tutorial1perso;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    //    private String bootstrapServer = "194.168.4.100:9092";
    private String bootstrapServer = "localhost:9092";

    private static Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public void sendData(KafkaProducer<String, String> producer) {
        String topic = "first_topic";
        String key = "id_";
        String message = "Message no ";
        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key + i, message + i);

//            LOGGER.info("Key for message " + key + i);
            //start
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    LOGGER.info("Received metadata\n Topic " + recordMetadata.topic()
                            + "\n Partition " + recordMetadata.partition()
                            + "\n Offset " + recordMetadata.offset()
                            + "\n Timestamp " + recordMetadata.timestamp());
                } else {
                    LOGGER.error("Exception while producting", e);
                }
            });
            // .get();  makes synchronous for debugging, do not use
        }
    }

    public static void main(String[] args) {
        ProducerDemo producerDemo = new ProducerDemo();

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerDemo.bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer(prop);
        producerDemo.sendData(producer);

        // wait for sending the data
        producer.flush();
        producer.close();
    }

}
