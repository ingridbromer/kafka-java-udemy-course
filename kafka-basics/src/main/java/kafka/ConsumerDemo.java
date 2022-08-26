package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
       log.info("I am a Kafka Consumer!");

       String bootstrapServers = "127.0.0.1:29092";
       String groupId = "my-second-application";
       String topic = "demo_java";

       Properties properties = new Properties();
       properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
       properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
       properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
       properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
       // Read all the data - earliest (other ones: none/latest)
       properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

       // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe consumer to our topic(s)
        // To subscribe to multiple topics:
        consumer.subscribe(Arrays.asList(topic));
       // consumer.subscribe(Collections.singletonList(topic));

        // poll for new data
        // poll and get as many records as you can, during now
        // but if you don't have any reply from Kafka, then i'm willing to wait up to 1000 milliseconds to get me some records;
        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record : records){
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }

    }
}
