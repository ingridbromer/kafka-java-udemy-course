package io.conduktor.demos.kafka.wikimedia;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class WikiMediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:29092";

        // Create Producer Config

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Set high throughput producer configs:
        // How long to wait until send a batch;
        // Add more messages in the batch at the expense of latency;
        // Increase linger.ms and the producer will wait a few milliseconds for the batches to fill up before sending them.
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        // if a batch is filled before linger.ms, increase the batch size
        // 32 kilobytes
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        //Helpful if your messages are text based
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        String topic = "wikimedia.recentchange";
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // Start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }

}
