package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups2 {
    public static void main(String[] args) {
        //create Producer Properties
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups2.class);

        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String groupID = "my-fifth-application";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to a topic
        consumer.subscribe(Arrays.asList(topic));
        //poll for new data
        while (true) {
            ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : record) {
                logger.info("key: " + consumerRecord.key(), " ,value: " + consumerRecord.value());
                logger.info("partition: " + consumerRecord.partition() + " ,offset: " + consumerRecord.offset());
            }


        }

    }
}
