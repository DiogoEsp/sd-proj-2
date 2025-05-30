package fctreddit.impl.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class KafkaSubscriber {

    private static final long POLL_TIMEOUT = 1L;

    final KafkaConsumer<String, String> consumer;

    public KafkaSubscriber(KafkaConsumer<String, String> consumer, List<String> topics) {
        this.consumer = consumer;
        this.consumer.subscribe(topics);
    }

    static public KafkaSubscriber createSubscriber( String addr, List<String> topics) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, addr);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grp" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaSubscriber(new KafkaConsumer<String, String>(props), topics);
    }

    public interface SubscriberListener {
        void onReceive(String topic, String key, String value);
    }

    public void consume(SubscriberListener listener) {
        for (;;) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(POLL_TIMEOUT));
            for (ConsumerRecord<String, String> r : records) {
                listener.onReceive(r.topic(), r.key(), r.value());
            }
        }
    }

    public void start(RecordProcessor recordProcessor) {
        new Thread( () -> {
            for (;;) {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(POLL_TIMEOUT));
                for (ConsumerRecord<String, String> r : records) {
                    recordProcessor.onReceive(r);
                }
            }
        }).start();
    }

}
