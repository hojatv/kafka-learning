package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithCallbackDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //create Producer Properties
        Logger logger = LoggerFactory.getLogger(ProducerWithCallbackDemo.class);
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName() );
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i =0; i<10; i++) {
            //create a producer record
            String key = "id_"+String.valueOf(i);
            logger.info("key:{}" , key);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic" , key,"hello world" + Integer.toString(i));
            //Send Data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime I send message successfully or an exception is thrown
                    if (e == null) {
                        logger.info("Receive new metaData. \n" +
                                "Toppic: " + recordMetadata.topic() + " \n" +
                                "Partition: " + recordMetadata.partition() + " \n" +
                                "Offset: " + recordMetadata.offset() + " \n" +
                                "TimeStamp: " + recordMetadata.timestamp());


                    } else {
                        logger.error("Error while producing", e);

                    }
                }
            }).get();// if you dont make it synchrounus, all logger.info("key:{}" , key); are printed before we get RecordMetaData

        }
        producer.flush();
        producer.close();

    }
}
