package com.youyu.sparkStreaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.youyu.conf.ConfigurationManager;
import com.youyu.constant.Constants;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
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
 * Created by root on 2017/5/22.
 */
public class AppActiveUserStats {
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
        //kafkaParams.put("group.id", Constants.KAFKA_GROUP_ID);

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

        JavaDStream<JSONObject> dataDStream = receiveDStream.flatMap(new FlatMapFunction<Tuple2<String, String>,
                JSONObject>() {
            @Override
            public Iterable<JSONObject> call(Tuple2<String, String> tuple2) throws Exception {
                List<JSONObject> results = new ArrayList<JSONObject>();

                JSONObject jsonObject = JSON.parseObject(tuple2._2());
                results.add(jsonObject);

                return results;
            }
        });

        dataDStream.foreachRDD(new VoidFunction<JavaRDD<JSONObject>>() {
            @Override
            public void call(JavaRDD<JSONObject> rdd) throws Exception {
                SQLContext sqlContext = new SQLContext(rdd.context());

                JavaRDD<Row> appActiveUserRowRDD = rdd.flatMap(new FlatMapFunction<JSONObject, Row>() {
                    @Override
                    public Iterable<Row> call(JSONObject x) throws Exception {
                        List<Row> results = new ArrayList<>();

                        String time = x.getString("reportTime").substring(0, 13);
                        String pkg = x.getString("pkgId");
                        String key = x.getString("appKey");
                        String version = x.getString("appVersion");
                        String channel = x.getString("appChannel");
                        String clientId = x.getString("clientId");
                        String clientMd5 = x.getString("clientIdMd5");

                        results.add(RowFactory.create(time, pkg, key, version, channel, clientId, clientMd5));

                        return results;
                    }
                });

                List<StructField> fields = new ArrayList<StructField>();
                fields.add(DataTypes.createStructField("time", DataTypes.StringType, true));
                fields.add(DataTypes.createStructField("pkg", DataTypes.StringType, true));
                fields.add(DataTypes.createStructField("key", DataTypes.StringType, true));
                fields.add(DataTypes.createStructField("version", DataTypes.StringType, true));
                fields.add(DataTypes.createStructField("channel", DataTypes.StringType, true));
                fields.add(DataTypes.createStructField("clientId", DataTypes.StringType, true));
                fields.add(DataTypes.createStructField("clientMd5", DataTypes.StringType, true));

                StructType schema = DataTypes.createStructType(fields);

                DataFrame activeUserDF = sqlContext.createDataFrame(appActiveUserRowRDD, schema);

                activeUserDF.registerTempTable("active_user");

                DataFrame activeUserCountDF = sqlContext.sql("select " +
                        "time," +
                        "(case when pkg = 'null' or pkg = '' then 'null' else pkg end) pkg," +
                        "(case when key = 'null' or key = '' then 'null' else key end) key," +
                        "(case when version = 'null' or version = '' then 'null' else version end) version," +
                        "(case when channel = 'null' or channel = '' then 'null' else channel end) channel," +
                        "count(1) " +
                        "from active_user " +
                        "group by time,pkg,key,version,channel ");

                /*System.out.println("--------------------------");
                activeUserCountDF.show();
                System.out.println("--------------------------");*/

                activeUserCountDF.javaRDD().foreach(new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println("**********************");
                        System.out.println(row.get(1));
                        System.out.println("**********************");
                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
    }
}
