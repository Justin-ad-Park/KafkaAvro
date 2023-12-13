package justin.kafkaavro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class AvroProducerV2 {
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        configs.setProperty("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(configs);

// 버전1 스키마 전송
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

        GenericRecord avroRecord = new GenericData.Record(avroSchema1);
        avroRecord.put("orderTime", System.nanoTime());
        avroRecord.put("orderId", new Random().nextLong());
        avroRecord.put("itemId", UUID.randomUUID().toString());

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(TOPIC_NAME, avroRecord);
        producer.send(record);


// 버전2 스키마 전송
        String schema2 = "{"
                + "\"namespace\": \"myrecord\","
                + " \"name\": \"orders\","
                + " \"type\": \"record\","
                + " \"fields\": ["
                + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
                + "     {\"name\": \"orderId\",  \"type\": \"long\"},"
                + "     {\"name\": \"itemNo\", \"type\": \"int\"}"
                + " ]"
                + "}";

        Schema.Parser parser2 = new Schema.Parser();
        Schema avroSchema2 = parser2.parse(schema2);

        GenericRecord avroRecord2 = new GenericData.Record(avroSchema2);
        avroRecord2.put("orderTime", System.nanoTime());
        avroRecord2.put("orderId", new Random().nextLong());
        avroRecord2.put("itemNo", 123);

        ProducerRecord<String, GenericRecord> record2 = new ProducerRecord<>(TOPIC_NAME, avroRecord2);
        producer.send(record2);


        producer.flush();
        producer.close();
    }
}