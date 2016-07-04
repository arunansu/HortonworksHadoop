package spark.local.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class WordCountJava8 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("data.txt");
		JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")));
		JavaRDD<Integer> lineLengths = input.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2(s, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		lineLengths.persist(StorageLevel.MEMORY_ONLY());
	}

}
