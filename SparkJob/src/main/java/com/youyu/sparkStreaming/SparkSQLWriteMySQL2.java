package com.youyu.sparkStreaming;

import com.alibaba.fastjson.JSON;
import com.youyu.bean.BillImportRecord;
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
import org.apache.spark.sql.*;
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

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 错误解决:https://www.mail-archive.com/user@spark.apache.org/msg52795.html
 * Created by root on 2017/5/4.
 */
public class SparkSQLWriteMySQL2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo3").setMaster("local[2]");
        conf.set("spark.cassandra.connection.host", "192.168.1.88");

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

                JavaDStream<BillImportRecord> dataDStream = jsonDStream.map(new Function<String, BillImportRecord>() {
                    @Override
                    public BillImportRecord call(String s) throws Exception {
                        BillImportRecord billImportRecord = JSON.parseObject(s, BillImportRecord.class);

                        return billImportRecord;
                    }
                });

                dataDStream.foreachRDD(new VoidFunction<JavaRDD<BillImportRecord>>() {
                    @Override
                    public void call(JavaRDD<BillImportRecord> rdd) throws Exception {

                        SQLContext sqlContext = new SQLContext(rdd.context());

                        JavaRDD<Row> billRowRDD = rdd.flatMap(new FlatMapFunction<BillImportRecord, Row>() {
                            @Override
                            public Iterable<Row> call(BillImportRecord billImportRecord) throws Exception {
                                List<Row> results = new ArrayList<>();

                                String sessionID = billImportRecord.getSessionId();
                                String type = billImportRecord.getActionType();
                                String bank = billImportRecord.getBankCode();
                                String userName = billImportRecord.getUserName();
                                String result = billImportRecord.getResultCode();
                                String desc = billImportRecord.getResultDesc();
                                String time = DateUtil.formatTime(billImportRecord.getRequestDate());


                                results.add(RowFactory.create(sessionID, type, bank, userName, result, desc, time));

                                return results;
                            }
                        });

                        List<StructField> fields = new ArrayList<StructField>();
                        fields.add(DataTypes.createStructField("session_id", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("action_type", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("bank_code", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("user_name", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("result_code", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("result_desc", DataTypes.StringType, true));
                        fields.add(DataTypes.createStructField("time", DataTypes.StringType, true));

                        StructType schema = DataTypes.createStructType(fields);

                        DataFrame billDetailDF = sqlContext.createDataFrame(billRowRDD, schema);

                        billDetailDF.registerTempTable("bill");

                        DataFrame billDF = sqlContext.sql("select " +
                                "session_id sessionid, " +
                                "action_type actiontype, " +
                                "bank_code bankcode," +
                                "user_name username," +
                                "result_code resultcode, " +
                                "result_desc resultdesc, " +
                                "time " +
                                "from bill");

                        Map<String, String> dayMap = new HashMap<>();
                        dayMap.put("keyspace", "billspace");
                        dayMap.put("table", "bill_bank");

                        System.out.println("###########");
                        billDF.show();
                        System.out.println("###########");

                        //保存到cassandra表中
                        billDF.write().format("org.apache.spark.sql.cassandra").options(dayMap).mode(SaveMode.Append)
                                .save();

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
