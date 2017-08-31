package kafka.spark.cassandra;

import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class KafkaAvroConsumer {

	private static Injection<GenericRecord, byte[]> recordInjection;
	static {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(KafkaAvroProducer.USER_SCHEMA);
		recordInjection = GenericAvroCodecs.toBinary(schema);
	}

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
		consumer.subscribe(Arrays.asList("TestAvroTopic"));
		while (true) {
			ConsumerRecords<String, byte[]> avroRecords = consumer.poll(100);
			for (ConsumerRecord<String, byte[]> avroRecord : avroRecords) {
				GenericRecord record = recordInjection.invert(avroRecord.value()).get();
				System.out.printf("first: " + record.get("first").toString() + " last: " + record.get("last").toString()
						+ " age: " + record.get("age").toString());
			}
		}
	}

}
