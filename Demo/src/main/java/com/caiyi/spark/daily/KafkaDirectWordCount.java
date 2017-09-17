package com.caiyi.spark.daily;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Administrator
 *
 */
public class KafkaDirectWordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("KafkaDirectWordCount");  
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", 
				"192.168.1.107:9092,192.168.1.108:9092,192.168.1.109:9092");

		Set<String> topics = new HashSet<String>();
		topics.add("WordCount");

		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
				jssc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				topics);

		/*JavaDStream<String> words = lines.flatMap(
				
				new FlatMapFunction<Tuple2<String,String>, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<String> call(Tuple2<String, String> tuple)
							throws Exception {
						return Arrays.asList(tuple._2().split(" "));
					}
					
				});
		
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				
				new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String word) throws Exception {
						return new Tuple2<String, Integer>(word, 1);
					}
					
				});
		
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				
				new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
					
				});*/
		
		/*wordCounts.print();*/
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jssc.close();
	}
	
}
