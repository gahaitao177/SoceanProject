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
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by root on 2017/3/17.
 */
public class SparkUpdateByKeyWC {
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

                        while (itt.hasNext()) {
                            Connection conn = null;
                            PreparedStatement stmt = null;
                            String url = "jdbc:mysql://localhost:3306/test";
                            String user = "root";
                            String pwd = "123456";

                            conn = DriverManager.getConnection(url, user, pwd);

                            while (itt.hasNext()) {
                                Tuple2<String, Integer> tup2 = itt.next();

                                String sql2 = "SELECT value FROM wordcount WHERE name = '" +
                                        tup2._1() + "' ";

                                stmt = conn.prepareStatement(sql2);
                                ResultSet resultSet = stmt.executeQuery();

                                if (resultSet.next() == true) {
                                    String sql = "UPDATE wordcount SET VALUE = ? WHERE name = ?";

                                    stmt = conn.prepareStatement(sql);
                                    stmt.setInt(1, tup2._2());
                                    stmt.setString(2, tup2._1());
                                    int flag = stmt.executeUpdate();

                                    System.out.println("**************更新成功+" + flag + "+****************");
                                } else {
                                    String sql = "INSERT INTO wordcount (NAME,VALUE) values(?,?) ";

                                    stmt = conn.prepareStatement(sql);
                                    stmt.setString(1, tup2._1());
                                    stmt.setInt(2, tup2._2());
                                    int flag = stmt.executeUpdate();

                                    System.out.println("***************插入成功 +" + flag + " +***************");
                                }

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
