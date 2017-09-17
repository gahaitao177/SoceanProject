package com.youyu.filter;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by root on 2017/5/4.
 */
public class test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo3").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

        //获取zookeeper的地址
        String zkServer = "192.168.1.55:2181,192.168.1.61:2181,192.168.1.69:2181/dcos-service-kafka";
        final ZkClient zkClient = new ZkClient(zkServer);


        String kafkaAddr = "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460";
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaAddr);
        kafkaParams.put("group.id", "sessionGroupId");

        String topic = "bill_topic2";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        ZKGroupTopicDirs zgt = new ZKGroupTopicDirs("sessionGroupId", topic);
        final String zkTopicPath = zgt.consumerOffsetDir();
        int countChildren = zkClient.countChildren(zkTopicPath);

        Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();

        if (countChildren > 0) {
            for (int i = 0; i < countChildren; i++) {
                String path = zkTopicPath + "/" + i;

                String offset = zkClient.readData(path);
                TopicAndPartition topicAndPartition = new TopicAndPartition(topic, i);
                fromOffsets.put(topicAndPartition, Long.parseLong(offset));

                JavaInputDStream<String> lines = KafkaUtils.createDirectStream(
                        jsc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        String.class,
                        kafkaParams,
                        fromOffsets,
                        new Function<MessageAndMetadata<String, String>, String>() {
                            @Override
                            public String call(MessageAndMetadata<String, String> v1) throws Exception {
                                return v1.message();
                            }
                        });

                JavaDStream<String> jsonDStream = lines.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);

                        return rdd;
                    }
                });

                JavaDStream<String> dataDStream = jsonDStream.map(new Function<String, String>() {
                    @Override
                    public String call(String s) throws Exception {
                        return s;
                    }
                });

                dataDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                    @Override
                    public void call(JavaRDD<String> rdd) throws Exception {
                        rdd.foreach(new VoidFunction<String>() {
                            @Override
                            public void call(String s) throws Exception {
                                System.out.println("过滤脏数据---->" + s);
                            }
                        });

                        ZkClient zkClient = new ZkClient(zkServer);
                        OffsetRange[] offsets = offsetRanges.get();

                        if (null != offsets) {
                            ZKGroupTopicDirs zgt = new ZKGroupTopicDirs("sessionGroupId", topic);

                            String zkTopicPath = zgt.consumerOffsetDir();
                            for (OffsetRange o : offsets) {
                                String zkPath = zkTopicPath + "/" + o.partition();
                                ZkUtils.updatePersistentPath(zkClient, zkPath, String.valueOf(o.untilOffset()));

                            }

                            zkClient.close();
                        }
                    }
                });


            }

        }


        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
