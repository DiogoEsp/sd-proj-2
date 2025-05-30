package fctreddit.impl.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaPublisher {

    private final KafkaProducer<String, String> producer;

    private KafkaPublisher( KafkaProducer<String, String> producer ) {
        this.producer = producer;
    }

    static public KafkaPublisher createPublisher(String addr) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, addr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaPublisher(new KafkaProducer<String, String>(props));
    }

    public long publish(String topic, String value) {
        try {
            Future<RecordMetadata> promise = producer.send(new ProducerRecord<String, String>(topic, value));
            RecordMetadata rec = promise.get(3, TimeUnit.SECONDS);
            System.out.println("Published to topic " + topic + " with offset " + rec.offset());
            return rec.offset();
        } catch (ExecutionException | InterruptedException | TimeoutException x) {
            System.out.println("Error: " + x.getMessage());
            return -1;
        }
    }
}
