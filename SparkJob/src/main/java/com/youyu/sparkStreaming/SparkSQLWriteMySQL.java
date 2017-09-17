package com.youyu.sparkStreaming;

import com.alibaba.fastjson.JSON;
import com.youyu.bean.BillImportRecord;
import com.youyu.utils.DateUtil;
import kafka.serializer.StringDecoder;
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
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * kafka + sparkStreaming + mysql
 * 问题描述：如果先启动程序再往kafka中发数据。数据不会丢失；如果先发数据后启动程序，数据会丢失
 * Created by root on 2017/5/4.
 */
public class SparkSQLWriteMySQL {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Demo2").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        String kafkaAddr = "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460";
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

                System.out.println("###########");
                billDF.show();
                System.out.println("###########");

                String url = "jdbc:mysql://localhost:3306/test";

                Properties prop = new Properties();
                prop.setProperty("user", "root");
                prop.setProperty("password", "123456");

                billDF.write().mode(SaveMode.Append).jdbc(url, "bill_bank", prop);

            }
        });


        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
