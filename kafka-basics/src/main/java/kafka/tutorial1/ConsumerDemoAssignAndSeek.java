package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        //create Producer Properties
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message
        //assign
        int partition_zero = 0;
        TopicPartition topicPartition = new TopicPartition(topic, partition_zero);
        consumer.assign(Arrays.asList(topicPartition));
        //seek
        long offsetToReadFrom = 15L;
        consumer.seek(topicPartition, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMEssagesReadSoFar = 0;



        //poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : record) {
                numberOfMEssagesReadSoFar++;
                logger.info("key: " + consumerRecord.key(), " ,value: " + consumerRecord.value());
                logger.info("partition: " + consumerRecord.partition() + " ,offset: " + consumerRecord.offset());
                if(numberOfMEssagesReadSoFar == numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }


        }
        logger.info("Exiting application");

    }
}
