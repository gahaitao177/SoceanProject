package com.youyu.sparkStreaming;

import com.alibaba.fastjson.JSON;
import com.youyu.bean.App;
import com.youyu.utils.DateUtil;
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

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static jdk.nashorn.internal.objects.NativeString.substr;

/**
 * Created by root on 2017/5/4.
 */
public class SparkSQLWriteMySQL3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo4").setMaster("local[2]");

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

                JavaDStream<App> dataDStream = jsonDStream.map(new Function<String, App>() {
                    @Override
                    public App call(String s) throws Exception {
                        App app = JSON.parseObject(s, App.class);

                        return app;
                    }
                });

                dataDStream.foreachRDD(new VoidFunction<JavaRDD<App>>() {
                    @Override
                    public void call(JavaRDD<App> rdd) throws Exception {

                        rdd.foreachPartition(new VoidFunction<Iterator<App>>() {
                            @Override
                            public void call(Iterator<App> itt) throws Exception {
                                Connection conn = null;
                                PreparedStatement stmt = null;
                                String url = "jdbc:mysql://localhost:3306/test";
                                String user = "root";
                                String pwd = "123456";

                                conn = DriverManager.getConnection(url, user, pwd);

                                Map<String, Integer> keyMap = new HashMap<>();


                                while (itt.hasNext()) {
                                    System.out.println("进入-----------------------1");
                                    App app = itt.next();
                                    String enterTime = DateUtil.formatTime(app.getEnterTime());
                                    String keyTimeCode = substr(enterTime, 0, 10);
                                    String hourCode = "d" + substr(enterTime, 11, 2);

                                    //将表中的key全部拿去出来放到Map中
                                    String sql1 = "select daycode from activeuser ";
                                    stmt = conn.prepareStatement(sql1);
                                    ResultSet rs = stmt.executeQuery();

                                    ResultSetMetaData rsmd = rs.getMetaData();
                                    int columnCount = rsmd.getColumnCount();

                                    while (rs.next()) {
                                        for (int i = 1; i <= columnCount; i++) {
                                            String dCode = rs.getString(i);
                                            keyMap.put(dCode, 1);
                                        }

                                    }

                                    for (Map.Entry<String, Integer> entry : keyMap.entrySet()) {
                                        System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                                    }

                                    boolean flag = keyMap.containsKey(keyTimeCode);

                                    if (flag) {
                                        System.out.println("##################1");
                                        String sql2 = "select " + hourCode + " from activeuser where daycode = '" +
                                                keyTimeCode + "' ";

                                        stmt = conn.prepareStatement(sql2);
                                        ResultSet resultSet = stmt.executeQuery();

                                        int value = 0;
                                        if (resultSet.next()) {
                                            value = resultSet.getInt(hourCode) + 1;
                                        }

                                        String sqlUpdate = "UPDATE activeuser SET " + hourCode + " = ? WHERE " +
                                                "daycode = ?";

                                        stmt = conn.prepareStatement(sqlUpdate);
                                        stmt.setInt(1, value);
                                        stmt.setString(2, keyTimeCode);

                                        stmt.executeUpdate();

                                    } else {
                                        System.out.println("*************1");
                                        String sqlInsert = "insert into activeuser values ( '" + keyTimeCode + "',0," +
                                                "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 )";

                                        stmt = conn.prepareStatement(sqlInsert);
                                        stmt.executeUpdate();

                                        //将当前这条数据插更新到表中
                                        String sqlUpdate = "UPDATE activeuser SET " + hourCode + " = ? WHERE " +
                                                "daycode = ?";

                                        stmt = conn.prepareStatement(sqlUpdate);
                                        stmt.setInt(1, 1);
                                        stmt.setString(2, keyTimeCode);
                                        stmt.executeUpdate();

                                    }

                                }
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
