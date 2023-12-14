package justin.kafka.avro.generatedobject;

import example.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * $ kafka-topics --bootstrap-server localhost:9092 --topic test_users -create
 */

public class AvroObjectProducer {
    private final static String TOPIC_NAME = "test_users";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");


        Producer<String, User> producer = new KafkaProducer<>(props);

        User user = new User();
        user.setName("Justin");
        user.setFavoriteColor("Red");
        user.setFavoriteNumber(new Random().nextInt());

        ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC_NAME, user.getName(), user);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}