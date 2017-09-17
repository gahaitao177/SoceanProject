package com.youyu.blackName;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Created by root on 2017/6/30.
 */
public class BlackNameWSL {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BlackNameWSL").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //HiveContext hiveContext = new HiveContext(sc.sc());

        //HDFS
        //JavaRDD<String> linesRDD = sc.textFile("hdfs://192.168.1.46/user/gaoht/file/black_22.txt");

        //本地
        JavaRDD<String> linesRDD = sc.textFile("D://WSL//hemingdan.txt");

        JavaRDD<JSONObject> jsonObjectRDD = linesRDD.map(new Function<String, JSONObject>() {
            @Override
            public JSONObject call(String s) throws Exception {
                s = s.replace(": 黑名单请求:", "\",\"黑名单请求\":\"");
                JSONObject jsonObject = JSON.parseObject(s);
                return jsonObject;
            }
        });

        JavaRDD<String> resultRDD = jsonObjectRDD.map(new Function<JSONObject, String>() {
            @Override
            public String call(JSONObject jsonObject) throws Exception {
                String blackName = jsonObject.getString("黑名单请求");
                String enter_time = jsonObject.getString("log").substring(0, 10);
                JSONObject blackNameJson = JSON.parseObject(blackName);
                String channelType = blackNameJson.getString("channelType");
                String cphone = blackNameJson.getString("cphone");
                String ipAddr = blackNameJson.getString("ipAddr");
                String yzmType = blackNameJson.getString("yzmType");

                StringBuffer buffer = new StringBuffer();
                buffer.append("黑名单请求" + "\t" + ipAddr + "\t" + cphone + "\t" + channelType + "\t" + yzmType + "\t" +
                        enter_time);

                String result = buffer.toString();

                return result;
            }
        });

        jsonObjectRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {
                String blackName = jsonObject.getString("黑名单请求");
                String enter_time = jsonObject.getString("log").substring(0, 10);

                JSONObject blackNameJson = JSON.parseObject(blackName);
                String channelType = blackNameJson.getString("channelType");
                String cphone = blackNameJson.getString("cphone");
                String ipAddr = blackNameJson.getString("ipAddr");
                String yzmType = blackNameJson.getString("yzmType");

                System.out.println(enter_time + "-" + channelType + "-" + cphone);
            }
        });

        //resultRDD.coalesce(1).saveAsTextFile("D://WSL//black-name.txt");
        resultRDD.coalesce(1).saveAsTextFile("hdfs://192.168.1.46/user/gaoht/black/black_filter_20170703");

        /*hiveContext.sql("use tmp;CREATE TABLE IF NOT EXISTS black_name_ght (name String,ip string,ipone string," +
                "channelType " +
                "string,yzmType string,date string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE" +
                " " +
                " ");

        hiveContext.sql("use tmp;load data local inpath " +
                "'hdfs://192.168.1.46/user/gaoht/black/black_filter_22/part-00000' " +
                "into table black_name_ght ");

        DataFrame countDF = hiveContext.sql("select count(1) from black_name_ght ");

        countDF.show();*/


        sc.close();
    }
}
