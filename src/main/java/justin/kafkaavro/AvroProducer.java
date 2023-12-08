package justin.kafkaavro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class AvroProducer {
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        configs.setProperty("schema.registry.url", "http://localhost:8081");

        String schema = "{"
                + "\"namespace\": \"myrecord\","
                + " \"name\": \"orders\","
                + " \"type\": \"record\","
                + " \"fields\": ["
                + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
                + "     {\"name\": \"orderId\",  \"type\": \"long\"},"
                + "     {\"name\": \"itemId\", \"type\": \"string\"}"
                + " ]"
                + "}";

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema1 = parser.parse(schema);

        // generate avro generic record
        GenericRecord avroRecord = new GenericData.Record(avroSchema1);
        avroRecord.put("orderTime", System.nanoTime());
        avroRecord.put("orderId", new Random().nextLong());
        avroRecord.put("itemId", UUID.randomUUID().toString());

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(configs);
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(TOPIC_NAME, avroRecord);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}