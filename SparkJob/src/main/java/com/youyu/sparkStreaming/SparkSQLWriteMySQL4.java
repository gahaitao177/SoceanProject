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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * http://www.jianshu.com/p/ccba410462ba
 * http://www.tuicool.com/articles/z2IBfq3
 * https://my.oschina.net/leejun2005/blog/172565
 * http://blog.csdn.net/q79969786/article/details/42793487
 * Created by root on 2017/5/4.
 */
public class SparkSQLWriteMySQL4 {
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

                        SQLContext sqlContext = new SQLContext(rdd.context());

                        JavaRDD<Row> appRowRDD = rdd.flatMap(new FlatMapFunction<App, Row>() {
                            @Override
                            public Iterable<Row> call(App app) throws Exception {
                                List<Row> results = new ArrayList<>();

                                String deviceId = app.getDeviceId();
                                String appKey = app.getAppKey();
                                String deviceType = app.getDeviceType();
                                String deviceOs = app.getDeviceOs();
                                String deviceModel = app.getDeviceModel();
                                String deviceBrand = app.getDeviceBrand();
                                String appVersion = app.getAppVersion();
                                String appName = app.getAppName();
                                String appSource = app.getAppSource();
                                String appChannel = app.getAppChannel();
                                String appNetWork = app.getAppNetWork();
                                String cityName = app.getCityName();
                                String userId = app.getUserId();
                                String userName = app.getUserName();
                                String enterTime = DateUtil.formatTime(app.getEnterTime());


                                results.add(RowFactory.create(deviceId, appKey, deviceType, deviceOs, deviceModel,
                                        deviceBrand, appVersion, appName, appSource, appChannel, appNetWork,
                                        cityName, userId, userName, enterTime));

                                return results;
                            }
                        });

                        List<StructField> fields = new ArrayList<StructField>();
                        fields.add(DataTypes.createStructField("deviceId", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("key", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("os", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("model", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("brand", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("version", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("appname", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("source", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("channel", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("network", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("cityname", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("userid", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("username", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("time", DataTypes.StringType, true));

                        StructType schema = DataTypes.createStructType(fields);

                        DataFrame appDetailDF = sqlContext.createDataFrame(appRowRDD, schema);

                        appDetailDF.registerTempTable("app_profile");

                        DataFrame appDF = sqlContext.sql("" +
                                "select deviceId,substr(time,0,10),concat('d',lpad(hour(time),2,0)), count(1) " +
                                "from app_profile " +
                                "group by deviceId,substr(time,0,10),concat('d',lpad(hour(time),2,0)) " +
                                "");

                        System.out.println("-------------------");
                        appDF.show();
                        System.out.println("-------------------");

                        appDF.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
                            @Override
                            public void call(Iterator<Row> itt) throws Exception {

                                String url = "jdbc:mysql://localhost:3306/test";
                                String user = "root";
                                String pwd = "123456";

                                Connection conn = DriverManager.getConnection(url, user, pwd);

                                PreparedStatement stmt = null;

                                Map<String, String> keyMap = new HashMap<>();

                                while (itt.hasNext()) {
                                    Row row = itt.next();
                                    String rowKey = row.getString(0);

                                    //将表中的所有主键都拿取出来
                                    String sql1 = "select daycode from activeuser ";
                                    conn.prepareStatement(sql1);
                                    ResultSet resultSet = stmt.executeQuery();

                                    //keyMap.put(resultSet.getString("daycode"), "1");

                                    /*if (keyMap.containsKey(rowKey)) {//表中存在当前的key
                                        System.out.println("后续开发......");
                                    } else {//表中不存在当前的key
                                        String sqlInsert = "insert into activeuser values ( " + rowKey + "," + 0 + "," +
                                                0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," +
                                                0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," +
                                                0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," +
                                                0 + "," + 0 + "," + ")";
                                        stmt = conn.prepareStatement(sqlInsert);
                                        int flag = stmt.executeUpdate();
                                        System.out.println("更新是否成功：" + flag);

                                        //将当前这条数据插更新到表中
                                        String sqlUpdate = "UPDATE activeuser SET " + row.getString(2) + " = ? WHERE " +
                                                "daycode = ?";

                                        stmt.setInt(1, row.getInt(3));
                                        stmt.setString(2, rowKey);
                                        stmt.executeUpdate();

                                    }*/


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
