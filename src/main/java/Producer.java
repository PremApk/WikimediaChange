import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) {

        String bootstrapConduktorServer = "cluster.playground.cdkt.io:9092";
        String topic = "wikimedia-change";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";


        //Setting up the properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapConduktorServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Conduktor Security Settings
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "XXXXXXX");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //Below properties needs to added if kafka <= 2.0
        //Above kafka 3.x -> Enabled by default
        //These properties will enhance Kafka Producer behaviour

//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));


        //Set High throughput Producer Config
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //using Snappy Compression

        //Create Kafka Producer Object
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //EventSource creation using Okhttp3 eventSource
        EventHandler eventHandler = new WikimediaEventHandler(kafkaProducer, topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //Start the producer in another thread
        eventSource.start();

        //Since the eventSource is running in another thread, we have to make the mainThread that runs Producer code
        //Should not terminate. So block the main Thread with some timeout.
        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }
}
