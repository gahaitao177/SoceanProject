package com.youyu.hbase;

import com.alibaba.fastjson.JSON;
import com.youyu.bean.AppProfile;
import com.youyu.conf.ConfigurationManager;
import com.youyu.constant.Constants;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * http://www.aboutyun.com/thread-12123-1-1.html
 * http://www.cnblogs.com/xlturing/p/spark.html
 * Created by root on 2017/5/9.
 */
public class RealtimeQueryHbase {
    public static void main(String[] args) {
        final String tableName = ConfigurationManager.getProperty(Constants.HBASE_TABLE_INFO_NAME);

        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local[1]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        Broadcast<String> broadcastTableName = jsc.sparkContext().broadcast(tableName);

        String kafkaAddr = ConfigurationManager.getProperty(Constants.KAFKA_LIST);

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaAddr);

        String topic = ConfigurationManager.getProperty(Constants.KAFKA_TOPIC_NAME);
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        JavaPairInputDStream<String, String> receiveDStream = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        JavaDStream<String> jsonDStream = receiveDStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2();
            }
        });

        JavaDStream<AppProfile> dataDStream = jsonDStream.map(new Function<String, AppProfile>() {
            @Override
            public AppProfile call(String s) throws Exception {
                Map<String, Object> map = JSON.parseObject(s);

                map.remove("histories");
                map.remove("events");
                map.remove("starts");

                String json = JSON.toJSONString(map);

                System.out.println("过滤后的json" + json);

                AppProfile appProfile = JSON.parseObject(json, AppProfile.class);
                return appProfile;
            }
        });

        dataDStream.foreachRDD(new VoidFunction<JavaRDD<AppProfile>>() {
            @Override
            public void call(JavaRDD<AppProfile> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<AppProfile>>() {
                    @Override
                    public void call(Iterator<AppProfile> itt) throws Exception {
                        while (itt.hasNext()) {

                            String rowKey = "app#active_user#2017-05-10#com.youyu.yystat#2.4.0#百度";
                            String hourCode = "d01";

                            long count = HbaseUtils.queryColumnValues(tableName, rowKey, "data", hourCode, 0L);

                            System.out.println("2017-05-12 01 实时数据" + count);

                        }
                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
