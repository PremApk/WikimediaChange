import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {


    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(),
                    connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapConduktorServer = "cluster.playground.cdkt.io:9093";
        String groupId = "wikimedia-app";

        //Properties for kafka Client
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapConduktorServer);
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "XXXXXXXXXXX");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //Setting up the De-Serializers
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<String, String>(properties);
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());
        String INDEX = "wikimedia";
        String topic = "trial";

        //Create OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //KafkaConsumer Creation
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //Subscribe to the topic
        consumer.subscribe(Arrays.asList(topic));
        //Create Index in OpenSearch if it doesn't exist
        try (openSearchClient) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(INDEX), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Index created successfully");
            } else {
                log.info("Index already exists");
            }
        } catch (Exception e) {
            log.error("Exception occured : " + e);
        }


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            int recordCount = records.count();
            log.info("Received " + recordCount + " records");

            for (ConsumerRecord<String, String> record : records) {
                IndexRequest indexRequest = new IndexRequest("wikimedia")
                        .source(record.value(), XContentType.JSON);

                IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                log.info(response.getId());
            }
        }


    }
}
