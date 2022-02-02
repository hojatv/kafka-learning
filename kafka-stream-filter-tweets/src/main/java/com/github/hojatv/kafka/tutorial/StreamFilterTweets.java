package com.github.hojatv.kafka.tutorial;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamFilterTweets {
    public static Logger logger = LoggerFactory.getLogger(StreamFilterTweets.class.getName());

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        /*properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());*/

        //create a topology
        StreamsBuilder streamBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamBuilder.stream("twitter_tweets");
        KStream<String, String> filterStream = inputTopic.filter(
                (k, jsonTweet) ->
                        //filter for tweets which has a user of over 1000 followers
                        extractFollowersCountFromTweet(jsonTweet) > 1000
        );
        filterStream.to("important_tweets");
        //build the topology
        try {
            KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder.build(), properties);
            //start our streams application
            kafkaStreams.start();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractFollowersCountFromTweet(String tweetJson) {

        int numberOfFollowers = 0;
        try {

            numberOfFollowers = jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception e) {
            logger.error("Invalid tweet. ", e);
        }

        return numberOfFollowers;
    }
}
