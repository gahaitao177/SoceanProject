package com.caiyi.spark.daily;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by root on 2016/12/15.
 */
public class SparkCassandra {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("TestSparkCassandra")
                .setMaster("local[2]");

        conf.set("spark.cassandra.connection.host", "192.168.2.180");

        //JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        SparkContext sc = new SparkContext(conf);

        /*Dataset billDF = cassandraSQLContext.sql("select * from demospace.bill;");

        JavaPairRDD<String, String> billSessionIDPairRDD = billDF.javaRDD().mapToPair(new PairFunction<Row, String,
                String>() {

            public Tuple2<String, String> call(Row row) throws Exception {
                String sessionID = row.getString(0);
                String type = row.getString(1);
                String result = row.getString(1);
                Date time = row.getDate(3);
                String bank = row.getString(4);
                String detail = row.getString(5);
                String userName = row.getString(6);

                String billDetailSession = sessionID + "," + type + "," + result + "," + time + "," + bank + "," +
                        detail + "," + userName;
                return new Tuple2<String, String>(sessionID, billDetailSession);
            }
        });

        billSessionIDPairRDD.groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            public void call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                System.out.println("key" + tuple2._1() + "  value" + tuple2._2());
            }
        });*/

        sc.stop();
    }
}
