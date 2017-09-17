package com.youyu.sparkStreaming;

import com.alibaba.fastjson.JSON;
import com.youyu.bean.AppProfile;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.*;
import java.util.*;

import static jdk.nashorn.internal.objects.NativeString.substr;

/**
 * Created by root on 2017/5/9.
 */
public class AppRealtimeAnalysis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AppRealtimeAnalysis").setMaster("local[1]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        String kafkaAddr = "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460";
        String zkAddr = "192.168.1.55:2181,192.168.1.61:2181,192.168.1.69:2181/dcos-service-kafka";

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaAddr);

        String topic = "bill_topic2";
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

                        Connection conn = null;
                        PreparedStatement stmt = null;
                        String url = "jdbc:mysql://localhost:3306/test";
                        String user = "root";
                        String pwd = "123456";

                        conn = DriverManager.getConnection(url, user, pwd);

                        Map<String, Integer> keyMap = new HashMap<>();
                        Map<String, Integer> clientMap = new HashMap<>();

                        while (itt.hasNext()) {
                            AppProfile appProfile = itt.next();

                            String enterTime = appProfile.getReportTime();
                            String keyTimeCode = substr(enterTime, 0, 10);
                            String hourCode = "d" + substr(enterTime, 11, 2);
                            String clientKey = appProfile.getClientId() + keyTimeCode + hourCode;

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

                            boolean flag = keyMap.containsKey(keyTimeCode);

                            if (flag) {
                                //判断当前这条数据所属的用户在规定时间内是否是第一次出现
                                String sqlClient = "select client_key from app_client_key ";
                                stmt = conn.prepareStatement(sqlClient);
                                ResultSet rst = stmt.executeQuery();

                                ResultSetMetaData rstmd = rst.getMetaData();
                                int count = rstmd.getColumnCount();

                                while (rst.next()) {
                                    for (int i = 1; i <= count; i++) {
                                        String clientCode = rst.getString(i);
                                        clientMap.put(clientCode, 1);
                                    }

                                }

                                boolean clientFlag = clientMap.containsKey(clientKey);

                                if (clientFlag) {
                                    System.out.println("用户在统计时间段内重复出现！");
                                } else {
                                    //将用户在统计周期内出现的唯一表示插入到app_client_key表中
                                    String sqlClientInsert = "insert into app_client_key VALUES ( '" + clientKey + "'" +
                                            " )";

                                    stmt = conn.prepareStatement(sqlClientInsert);
                                    stmt.executeUpdate();

                                    //对用户第一次在统计周期内出现的次数进行统计累加到活跃用户表中
                                    String sql2 = "select " + hourCode + " from activeuser where daycode = '" +
                                            keyTimeCode + "' ";

                                    stmt = conn.prepareStatement(sql2);
                                    ResultSet resultSet = stmt.executeQuery();

                                    int value = 0;
                                    if (resultSet.next()) {
                                        value = resultSet.getInt(hourCode) + 1;
                                    }

                                    String sqlUpdate = "UPDATE activeuser SET " + hourCode + " = ? WHERE daycode = ?";

                                    stmt = conn.prepareStatement(sqlUpdate);
                                    stmt.setInt(1, value);
                                    stmt.setString(2, keyTimeCode);

                                    stmt.executeUpdate();
                                }

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
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
