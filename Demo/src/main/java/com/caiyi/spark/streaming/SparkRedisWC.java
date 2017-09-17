package com.caiyi.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;

/**
 * Created by root on 2017/3/16.
 */
public class SparkRedisWC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        String kafkaAddr = "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460";
        String topic = "bill_topic";

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", kafkaAddr);
        kafkaParams.put("group.id", "session_group_id");

        Set<String> topics = new HashSet<String>();
        topics.add(topic);

        JavaPairInputDStream<String, String> inputDS = KafkaUtils.createDirectStream(jsc, String.class, String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams, topics);

        JavaDStream<String> lineDS = inputDS.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2();
            }
        });

        JavaPairDStream<String, Integer> pairDS = lineDS.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] arr = s.split(",");
                String key = arr[0];
                Integer value = Integer.valueOf(arr[1]);
                return new Tuple2<String, Integer>(key, value);
            }
        });

        JavaPairDStream<String, Integer> countWordDS = pairDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        countWordDS.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    public void call(Iterator<Tuple2<String, Integer>> itt) throws Exception {
                        Jedis jedis = new Jedis("127.0.0.1", 6379);

                        while (itt.hasNext()) {
                            Tuple2<String, Integer> tup2 = itt.next();

                            String value = jedis.hget("wordcount", tup2._1());
                            if (null != value) {
                                jedis.hset("wordcount", tup2._1(), String.valueOf(Integer.valueOf(value) + tup2._2()));
                            } else {
                                jedis.hset("wordcount", tup2._1(), String.valueOf(tup2._2()));
                            }

                        }

                    }
                });
            }
        });


        countWordDS.print();

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jsc.stop();
    }
}
