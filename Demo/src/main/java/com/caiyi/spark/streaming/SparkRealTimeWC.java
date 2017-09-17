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
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by root on 2017/3/16.
 */
public class SparkRealTimeWC {
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
                        Connection conn = null;
                        PreparedStatement stmt = null;
                        String url = "jdbc:mysql://localhost:3306/test";
                        String user = "root";
                        String pwd = "123456";

                        conn = DriverManager.getConnection(url, user, pwd);

                        while (itt.hasNext()) {
                            Tuple2<String, Integer> tup2 = itt.next();

                            String sql2 = "select value from wordcount where name = '" +
                                    tup2._1() + "' ";

                            stmt = conn.prepareStatement(sql2);
                            ResultSet resultSet = stmt.executeQuery();

                            if (resultSet.next() == true) {
                                int value = resultSet.getInt("value") + tup2._2();

                                String sql = "UPDATE wordcount SET VALUE = ? WHERE name = ?";

                                stmt = conn.prepareStatement(sql);
                                stmt.setInt(1, value);
                                stmt.setString(2, tup2._1());
                                int flag = stmt.executeUpdate();

                                System.out.println("**************更新成功+" + flag + "+****************");
                            } else {
                                String sql = "insert into wordcount (NAME,VALUE) values(?,?) ";

                                stmt = conn.prepareStatement(sql);
                                stmt.setString(1, tup2._1());
                                stmt.setInt(2, tup2._2());
                                int flag = stmt.executeUpdate();

                                System.out.println("***************插入成功 +" + flag + " +***************");
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
