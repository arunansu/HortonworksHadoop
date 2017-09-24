package kafka.spark.cassandra;

import java.time.LocalDate;
import java.util.Properties;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
public class KafkaAvroProducer {
	public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"first\", \"type\":\"string\" },"
            + "  { \"name\":\"last\", \"type\":\"string\" },"
            + "  { \"name\":\"age\", \"type\":\"int\" }"
            + "]}";

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);

        for (int i = 1; i <= 10; i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("first", "first" + i);
            avroRecord.put("last", "last" + i);
            avroRecord.put("age", i);

            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>("TestAvroTopic", bytes);
            producer.send(record);

            Thread.sleep(250);
        }
        producer.close();
	}
}
