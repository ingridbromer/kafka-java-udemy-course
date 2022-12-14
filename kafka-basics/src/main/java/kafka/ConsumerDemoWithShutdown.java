package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
       log.info("I am a Kafka Consumer!");

       String bootstrapServers = "127.0.0.1:29092";
       String groupId = "my-third-application";
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

        // Get a referent to the current thread
        final Thread mainThread =  Thread.currentThread();

        Runtime.getRuntime().addShutdownHook( new Thread(){
            public void run(){
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // Join the main thread to allow the execution of the code in the main thread

                try{
                    mainThread.join();
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try {
        // Subscribe consumer to our topic(s)
        // To subscribe to multiple topics:
        consumer.subscribe(Arrays.asList(topic));
       // consumer.subscribe(Collections.singletonList(topic));

        // poll for new data
        // poll and get as many records as you can, during now
        // but if you don't have any reply from Kafka, then i'm willing to wait up to 1000 milliseconds to get me some records;
        while(true) {
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
        } catch(WakeupException e){
            log.info("Wake up exception!");
            // we ignore this as this in an exception when closing a consumer
            }
        catch(Exception e){
            log.error("Unexpected exception!");
        }
        finally{
            // Gracefully the consumer and the connection with Kafka
            // This will also commit the offsets if need be
            consumer.close();
            log.info("The consumer is now gracefully closed!");
        }

    }
}
