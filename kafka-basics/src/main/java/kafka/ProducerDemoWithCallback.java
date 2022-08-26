package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
       log.info("I am a Kafka Producer!");

        Properties properties = new Properties();
        // Properties.setProperty("key", "value");

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        // Transform String for binary
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

       // Create Producer Properties
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0; i < 10; i++) {
        // Create a Producer Record

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world " + i);

        // Send the data - asynchronous operation
        // Send ten records
            //Sticky Partitioner (Performance Improvement - Batches of Messages)
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // Executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //the record was successfully sent
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }

            });

            // Wait 1000 millis to change the partition
            try{
                Thread.sleep(1000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        // Flush data - synchronous operation - block on this line
        // of code up until all the data in my producer being sent

        producer.flush();
        // Flush and Close the Producer

        producer.close();


    }
}
