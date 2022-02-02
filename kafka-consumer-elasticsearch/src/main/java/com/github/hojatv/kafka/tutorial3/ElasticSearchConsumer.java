package com.github.hojatv.kafka.tutorial3;

import com.fasterxml.jackson.dataformat.smile.SmileParser;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException, InterruptedException {
        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();
        elasticSearchConsumer.run();
    }

    private void run() throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        String topic = "twitter_tweets";
            KafkaConsumer<String,String> kafkaConsumer = createKafkaConsumer(topic);

        //poll for new data
        BulkRequest bulkRequest = new BulkRequest();
        while (true) {

            ConsumerRecords<String, String> record = kafkaConsumer.poll(Duration.ofMillis(100));
            int count = record.count();
            logger.info("Received " + count + " records");
            for (ConsumerRecord<String, String> consumerRecord : record) {
                /*logger.info("key: " + consumerRecord.key(), " ,value: " + consumerRecord.value());
                logger.info("partition: " + consumerRecord.partition() + " ,offset: " + consumerRecord.offset());*/

                //making idempotent : if we reprocess same message twice, we don't insert it again in ES
                //2strategies for id generation
                //String id = consumerRecord.topic() +"_"+ consumerRecord.partition() +"_"+ consumerRecord.offset();

                //twitter feed specific id
                String id = extractIdFromTweet(consumerRecord.value());


                //insert to ES
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id// to make our consumer idempotent
                ).source(consumerRecord.value(), XContentType.JSON);


                /*IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String documentID = indexResponse.getId();
                logger.info("New ES document id: {}", documentID);*/
                bulkRequest.add(indexRequest);//we add to bulk request
            }
            if(count > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets..");
                kafkaConsumer.commitSync();
                logger.info("Offsets have been Committed.");
            }

        }

        //client.close();

    }

    private JsonParser jsonParser = new JsonParser();
    private String extractIdFromTweet(String tweetJson) {
        String id = jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

        return id;
    }

    public KafkaConsumer<String,String> createKafkaConsumer(String topic){
        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        String groupID = "kafka-demo-elasticsearch";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");


        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public RestHighLevelClient createClient() {
        String hostname = "abshar-554503681.eu-central-1.bonsaisearch.net";
        String username = "828mwqmipw";
        String password = "6ezomvebas";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;

    }
}
