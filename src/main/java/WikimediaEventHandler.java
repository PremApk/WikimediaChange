import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaEventHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    private final static Logger log = LoggerFactory.getLogger(WikimediaEventHandler.class.getSimpleName());

    public WikimediaEventHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        //Do  nothing
    }

    @Override
    public void onClosed() {
        //On Stream Closure, Close KafkaProducer too
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info("Message Data from Stream : " + messageEvent.getData());
        //Send Stream Data to the specified kafka Topic
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s){
        //Do nothing
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in Stream Data read  : " + throwable);
    }
}
