package kafka.spark.cassandra;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

public class KafkaAvroSparkCassandraConsumer {
	private static Injection<GenericRecord, byte[]> recordInjection;
	static Cluster cluster;
	static Session session;
	static {
	    Schema.Parser parser = new Schema.Parser();
	    Schema schema = parser.parse(KafkaAvroProducer.USER_SCHEMA);
	    recordInjection = GenericAvroCodecs.toBinary(schema);
		// Connect to the cluster and keyspace "test"
	  	cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	  	session = cluster.connect("test");
	}
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Set<String> topics = Collections.singleton("TestAvroTopic");
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(avroRecord -> {
                GenericRecord record = recordInjection.invert(avroRecord._2).get();

                UUID id = UUID.randomUUID();
                String first = record.get("first").toString();
                String last = record.get("last").toString();
                int age = Integer.valueOf(record.get("age").toString());
                Date date = new Date();
                System.out.println("first= " + first
                        + ", last= " + last
                        + ", age=" + age);
                
                Insert insert = QueryBuilder
        	            .insertInto("test", "person")
        	            .value("id", id)
        	            .value("active", true)
        	            .value("first", first)
        	            .value("last", last)
        	            .value("age", age)
        	            .value("created", date)
        	            .value("updated", date);
                session.execute(insert.toString()).all();
            });
        });

        ssc.start();
        ssc.awaitTermination();
	}
}
