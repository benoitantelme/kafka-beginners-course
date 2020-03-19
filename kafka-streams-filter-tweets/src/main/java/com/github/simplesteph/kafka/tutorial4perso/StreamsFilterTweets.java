package com.github.simplesteph.kafka.tutorial4perso;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    private JsonParser jsonParser = new JsonParser();

    private Integer extractUserFollowersInTweet(String tweetJson) {
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }


    public static void main(String[] args) {
        StreamsFilterTweets sft = new StreamsFilterTweets();
        Properties prop = new Properties();
        prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        prop.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        prop.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream("tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> sft.extractUserFollowersInTweet(jsonTweet) > 1000
        );
        filteredStream.to("popular_tweets");


        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                prop
        );

        // start our streams application
        kafkaStreams.start();

    }


}
