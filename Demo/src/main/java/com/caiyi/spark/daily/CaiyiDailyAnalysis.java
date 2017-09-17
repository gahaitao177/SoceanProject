package com.caiyi.spark.daily;

import com.alibaba.fastjson.JSON;
import com.caiyi.spark.conf.ConfigurationManager;
import com.caiyi.spark.constant.Constants;
import com.caiyi.spark.model.SessionDetail;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Socean on 2016/11/28.
 */
public class CaiyiDailyAnalysis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(Constants.SPARK_APP_NAME);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        Integer theadNum = ConfigurationManager.getInteger(Constants.KAFKA_NUM_THREADS);
        String kafkaTopicName = ConfigurationManager.getProperty(Constants.KAFKA_TOPIC_NAME);

        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put(kafkaTopicName, theadNum);

        String zkList = ConfigurationManager.getProperty(Constants.ZOOKEEPER_LIST);

        System.out.println("==========================================="+zkList);

        JavaPairReceiverInputDStream<String, String> linesDStream = KafkaUtils.createStream(jssc, zkList,
                Constants.KAFKA_GROUP_ID,
                topicThreadMap);

        System.out.println("--------------------------------------------0");
        JavaDStream<String> lineJSONDStream = linesDStream.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple) throws Exception {

                return tuple._2();
            }
        });

        System.out.println("---------------------------------------------1");

        JavaDStream<SessionDetail> sessionDetailDStream = lineJSONDStream.map(new Function<String, SessionDetail>() {

            public SessionDetail call(String lineJson) throws Exception {

                SessionDetail sessionDetail = JSON.parseObject(lineJson, SessionDetail.class);

                return sessionDetail;
            }
        });

        /*System.out.println("----------------------------------------------2");
        sessionDetailDStream.foreachRDD(new VoidFunction<JavaRDD<SessionDetail>>() {

            public void call(JavaRDD<SessionDetail> sessionDetailJavaRDD) throws Exception {
                sessionDetailJavaRDD.foreachPartition(new VoidFunction<Iterator<SessionDetail>>() {
                    public void call(Iterator<SessionDetail> sessionDetailIterator) throws Exception {

                    }
                });
            }
        });*/

        /*sessionDetailDStream.foreachRDD(new Function<JavaRDD<SessionDetail>, Void>() {

            public Void call(JavaRDD<SessionDetail> sessionDetailJavaRDD) throws Exception {

                sessionDetailJavaRDD.foreachPartition(new VoidFunction<Iterator<SessionDetail>>() {

                    public void call(Iterator<SessionDetail> sessionDetailIterator) throws Exception {

                        System.out.println("+++++++++++++++++++++++++++++++++++++");

                        *//*Connection conn = ConnectionPool.getConnection();
                        SessionDetail sessionDetail = null;


                        String sql = "insert into session (sessionId,type,bank,userName,result,detail,time) " +
                                "values ('0004','03','13','gaohaitao3','dd','12asd','2016-12-9 23:12:11');";

                        System.out.println("执行SQL语句成功" + sql);

                        Statement stat = conn.createStatement();
                        stat.executeUpdate(sql);*//*

                        *//*while (sessionDetailIterator.hasNext()) {

                            sessionDetail = sessionDetailIterator.next();

                            String sql = "insert into session (sessionId,type,bank,userName,result,detail,time) " +
                                    "values ('0004','03','13','gaohaitao3','dd','12asd','2016-12-9 23:12:11');";

                            System.out.println("执行SQL语句成功" + sql);

                            Statement stat = conn.createStatement();
                            stat.executeUpdate(sql);
                        }*//*

                        *//*ConnectionPool.returnConnection(conn);*//*
                    }
                });
                return null;
            }
        });*/

        Connection conn = ConnectionPool.getConnection();

        String sql = "insert into session (sessionId,type,bank,userName,result,detail,time) " +
                "values ('0009','03','13','gaohaitao3','dd','12asd','2016-12-9 23:12:11');";

        System.out.println("执行SQL语句成功" + sql);

        Statement stat = null;
        try {
            stat = conn.createStatement();
            stat.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ConnectionPool.returnConnection(conn);

        System.out.println("---------------------------------------------3");
        /*sessionDetailDStream.transform(new Function<JavaRDD<SessionDetail>, JavaRDD<String>>() {

            public JavaRDD<String> call(JavaRDD<SessionDetail> SessionDetailRDD) throws Exception {

                SessionDetailRDD.foreachPartition(new VoidFunction<Iterator<SessionDetail>>() {

                    public void call(Iterator<SessionDetail> sessionDetailIterator) throws Exception {

                        Connection conn = ConnectionPool.getConnection();
                        SessionDetail sessionDetail = null;

                        while(sessionDetailIterator.hasNext()){

                            sessionDetail = sessionDetailIterator.next();

                            String sql = "insert into session(sessionId,type,bank,userName,result,detail,time)"
                                    + "values ('"
                                    + sessionDetail.getSessionId()
                                    + "','"
                                    + sessionDetail.getType()
                                    + "','"
                                    + sessionDetail.getBank()
                                    + "','"
                                    + sessionDetail.getUserName()
                                    + "','"
                                    + sessionDetail.getResult()
                                    + "','"
                                    + sessionDetail.getDetail()
                                    + "','"
                                    + sessionDetail.getTime()
                                    + ")";
                            *//*Statement stat = conn.createStatement();
                            stat.executeUpdate(sql);*//*
                        }

                        ConnectionPool.returnConnection(conn);

                    }
                });
                return null;
            }
        });*/


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jssc.close();
    }
}
