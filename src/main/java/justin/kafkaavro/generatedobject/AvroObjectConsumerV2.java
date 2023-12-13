package justin.kafkaavro.generatedobject;

import example.avro.User;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AvroObjectConsumerV2 {
    private final static Logger logger = LoggerFactory.getLogger(AvroDeserializerConsumer.class);
    private final static String TOPIC_NAME = "test_users";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String GROUP_ID = "test-users-group-3";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", "true"); // Specific Avro Reader를 활성화

        Consumer<String, User> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, User> record : records) {
                User user = record.value();
                System.out.println("====");
                System.out.printf("offset = %d, key = %s, name = %s, color=%s, number=%d \n", record.offset(), record.key(), user.getName(), user.getFavoriteColor(), user.getFavoriteNumber());

            }
        }
    }
}
