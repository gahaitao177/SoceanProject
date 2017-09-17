package com.caiyi.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
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
 * Created by root on 2017/3/17.
 */
public class SparkRedisUpdateByKeyWC {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(2));
        jsc.checkpoint("D:\\checkpointDirct");

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
            public String call(Tuple2<String, String> tup2) throws Exception {
                return tup2._2();
            }
        });

        JavaPairDStream<String, Integer> pairDS = lineDS.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] arr = s.split(",");

                return new Tuple2<String, Integer>(arr[0], Integer.valueOf(arr[1]));
            }
        });

        JavaPairDStream<String, Integer> countWordDS = pairDS.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws
                            Exception {
                        Integer newValue = 0;
                        if (state.isPresent()) {
                            newValue = state.get();
                        }

                        for (Integer value : values) {
                            newValue += value;
                        }

                        return Optional.of(newValue);
                    }
                });

        countWordDS.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    public void call(Iterator<Tuple2<String, Integer>> itt) throws Exception {
                        Jedis jedis = new Jedis("127.0.0.1", 6379);

                        while (itt.hasNext()) {
                            Tuple2<String, Integer> tup2 = itt.next();
                            jedis.hset("wordcount", tup2._1(), String.valueOf(tup2._2()));
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
