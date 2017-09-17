package com.youyu.sparkStreaming;

import com.alibaba.fastjson.JSON;
import com.youyu.bean.AppProfile;
import com.youyu.conf.ConfigurationManager;
import com.youyu.constant.Constants;
import com.youyu.hbase.HbaseUtils;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
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
 * http://www.w2bc.com/article/214098
 * Created by root on 2017/5/9.
 */
public class AppRealtimeAnalysisHbase {
    public static void main(String[] args) {

        final String tableName = ConfigurationManager.getProperty(Constants.HBASE_TABLE_INFO_NAME);

        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local[1]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        Map<String, Integer> map = new HashMap<>();
        Broadcast<Map<String, Integer>> broadcastMap = jsc.sparkContext().broadcast(map);

        Broadcast<String> broadcastTableName = jsc.sparkContext().broadcast(tableName);

        String kafkaAddr = ConfigurationManager.getProperty(Constants.KAFKA_LIST);

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaAddr);
        kafkaParams.put("group.id", Constants.KAFKA_GROUP_ID);
        //kafkaParams.put("auto.offset.reset", "smallest");

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
                            AppProfile appProfile = itt.next();

                            String enterTime = appProfile.getReportTime();
                            String keyTimeCode = enterTime.substring(0, 10);
                            String hourCode = "d" + enterTime.substring(11, 13);

                            String pkg = appProfile.getPkgId();
                            String version = appProfile.getAppVersion();
                            String channel = appProfile.getAppChannel();
                            String activeDataType = "active_user";

                            String rowKey = "app#" + activeDataType + "#" + keyTimeCode + "#" + pkg + "#" + version +
                                    "#" + channel;

                            broadcastMap.value().put(rowKey, 1);

                            for (Map.Entry<String, Integer> entry : broadcastMap.value().entrySet()) {
                                System.out.println("----------------" + entry.getKey());
                            }

                            //处理活跃用户的数据
                            HbaseUtils.incrementColumnValues(broadcastTableName.value(), rowKey, "daily", hourCode, 2L);

                            ResultScanner results = HbaseUtils.getAllRows(broadcastTableName.value());

                            for (Result result : results) {
                                for (Cell cell : result.rawCells()) {
                                    System.out.println("行名：" + new String(CellUtil.cloneRow(cell)) +
                                            "\t\t时间戳：" + cell.getTimestamp() + " " +
                                            "\t列族名: " + new String(CellUtil.cloneFamily(cell)) +
                                            "\t列名：" + new String(CellUtil.cloneQualifier(cell)) +
                                            "\t\t值：" + new String(CellUtil.cloneValue(cell)));

                                }
                            }


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
