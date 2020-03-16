package kafka.tutorial1perso;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    private static String bootstrapServer = "localhost:9092";
    private static String groupId = "third_app";
    private static String topic = "first_topic";

    private static Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);


    public static void main(String[] args) {
        ConsumerDemoThread consumerDemoThread = new ConsumerDemoThread();
        consumerDemoThread.runThread();

    }

    public void runThread(){
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerRunnable = new ConsumerRunnable(bootstrapServer, groupId, topic, latch);

        Thread thread = new Thread(consumerRunnable);
        thread.start();

        //shutdown hook to clean close app
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Shutdown hook done");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Application got interrupted", e);
        } finally {
            LOGGER.info("Application is closing.");
        }
    }

    class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            Properties prop = new Properties();
            prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.latch = latch;
            this.consumer = new KafkaConsumer<>(prop);
            this.consumer.subscribe(List.of(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records)
                        LOGGER.info("Received record with key : " + record.key()
                                + "\nValue " + record.value()
                                + "\nPartition " + record.partition()
                                + "\nOffset " + record.offset());
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                {
                    consumer.close();
                    latch.countDown();
                }
            }
        }

        public void shutdown() {
            // interrupt consumer.poll() and will throw a WakeUpException
            consumer.wakeup();
        }
    }

}
