package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
       log.info("I am a Kafka Producer!");

        Properties properties = new Properties();
        // Properties.setProperty("key", "value");

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        // Transforme String for binary
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

       // Create Producer Properties
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create the Producer

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");

        // Send the data - asynchronous operation

        producer.send(producerRecord);
        // Flush data - synchronous operation - block on this line
        // of code up until all the data in my producer being sent

        producer.flush();
        // Flush and Close the Producer

        producer.close();


    }
}
