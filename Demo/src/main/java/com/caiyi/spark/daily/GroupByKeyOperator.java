package com.caiyi.spark.daily;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GroupByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GoupByKey").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> scoreList = Arrays.asList(
                new Tuple2<String, String>("session01", "1_1"),//1-2
                new Tuple2<String, String>("session01", "2_1"),
                new Tuple2<String, String>("session01", "5_1"),

                new Tuple2<String, String>("session02", "1_1"),//1-2
                new Tuple2<String, String>("session02", "2_0"),

                new Tuple2<String, String>("session03", "1_1"),//1-2
                new Tuple2<String, String>("session03", "2_1"),

                new Tuple2<String, String>("session04", "4_1"),//1-2-3
                new Tuple2<String, String>("session04", "2_1"),
                new Tuple2<String, String>("session04", "3_1"),
                new Tuple2<String, String>("session04", "1_1"),
                new Tuple2<String, String>("session04", "5_0"),

                new Tuple2<String, String>("session05", "5_1"),//2-3-4
                new Tuple2<String, String>("session05", "3_1"),
                new Tuple2<String, String>("session05", "2_1"),
                new Tuple2<String, String>("session05", "4_1"),

                new Tuple2<String, String>("session06", "2_1"),//2 用户中断

                new Tuple2<String, String>("session07", "1_1"),//1 用户中断

                new Tuple2<String, String>("session08", "1_0"),

                new Tuple2<String, String>("session09", "2_1"),//2-5-6-7
                new Tuple2<String, String>("session09", "5_1"),

                new Tuple2<String, String>("session10", "1_1"),//1-2-3 用户中断
                new Tuple2<String, String>("session10", "2_1"),
                new Tuple2<String, String>("session10", "3_1"),

                new Tuple2<String, String>("session11", "1_1"),//1-2-3
                new Tuple2<String, String>("session11", "2_1"),
                new Tuple2<String, String>("session11", "3_0"),

                new Tuple2<String, String>("session12", "2_0"), //2

                new Tuple2<String, String>("session13", "2_1"),//2-3
                new Tuple2<String, String>("session13", "3_0"),

                new Tuple2<String, String>("session14", "2_1"),//2-3 用户中断
                new Tuple2<String, String>("session14", "3_1")


        );


        JavaPairRDD<String, String> rdd = sc.parallelizePairs(scoreList);

        JavaPairRDD<String, String> aggrePairRDD = rdd.aggregateByKey("", new Function2<String, String, String>() {
            public String call(String s1, String s2) throws Exception {
                return s1 + "," + s2;
            }
        }, new Function2<String, String, String>() {
            public String call(String s1, String s2) throws Exception {
                return s1 + "," + s2;
            }
        });

        JavaPairRDD<String, String> bbRDD = aggrePairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,
                String>, String, String>() {
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> tuple2) throws Exception {
                List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();

                String key = tuple2._1();
                String[] arr = tuple2._2().substring(1, tuple2._2().length()).split(",");

                Arrays.sort(arr);

                StringBuffer buffer = new StringBuffer();
                for (int j = 0; j <= arr.length - 1; j++) {
                    if (j <= arr.length - 2) {
                        buffer.append(arr[j] + ",");
                    } else {
                        buffer.append(arr[j]);
                    }
                }

                results.add(new Tuple2<String, String>(key, buffer.toString()));

                return results.iterator();
            }
        });

        JavaPairRDD<String, Integer> ccRDD = bbRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>,
                String, Integer>() {
            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, String> tuple2) throws Exception {
                List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();

                String key = tuple2._1();
                String actions = tuple2._2();//获取算子传过来的value值: 1_1,2_1,5_1

                Integer flag = Integer.valueOf(actions.substring(0, 1));
                Integer length = actions.split("_|,").length;//获取当前value切分后的长度
                String actionFirst = actions.substring(0, 3);//1_1 or 2_1

                StringBuffer actionTypeBuf = new StringBuffer(); //拼接成功的标志比如：111 or 11 ...
                StringBuffer actionValueBuf = new StringBuffer(); //拼接用户操作的动作比如：123 or 23 ...

                Integer returnValue = 0;
                String successFlag = "1";

                if (flag == 1) {//判断用户一次会话中第一个动作是1
                    String actionType1 = null;
                    if (length == 2) {
                        actionType1 = actionFirst.split("_")[1];
                        if (successFlag.equals(actionType1)) {
                            returnValue = 1;
                        }
                    }

                    if (length == 6) {//1_1,2_1,3_1 判断所获取的字符串的长度中用户有三次动作
                        String[] actionArrs = actions.split("_|,");

                        String successType1 = "111";
                        String successValue1 = "123";

                        //拼接成111
                        actionTypeBuf.append(actionArrs[1]);
                        actionTypeBuf.append(actionArrs[3]);
                        actionTypeBuf.append(actionArrs[5]);

                        //拼接成123
                        actionValueBuf.append(flag);
                        actionValueBuf.append(actionArrs[2]);
                        actionValueBuf.append(actionArrs[4]);

                        if (successType1.compareTo(actionTypeBuf.toString()) == 0 && successValue1.compareTo
                                (actionValueBuf.toString()) == 0) {
                            returnValue = 1;
                        }

                    }
                }

                if (flag == 2) {//判断用户一次会话中第一个动作是2
                    if (length == 2) {
                        String actionType2 = null;
                        actionType2 = actionFirst.split("_")[1];
                        if (successFlag.equals(actionType2)) {
                            returnValue = 1;
                        }
                    }

                    if (length == 4) {//2_1,3_1 判断所获取的字符串的长度中用户有两次动作
                        String[] actionArrs = actions.split("_|,");

                        String successType2 = "11";
                        String successValue2 = "23";

                        //拼接成11
                        actionTypeBuf.append(actionArrs[1]);
                        actionTypeBuf.append(actionArrs[3]);

                        //拼接成12
                        actionValueBuf.append(flag);
                        actionValueBuf.append(actionArrs[2]);

                        if (successType2.compareTo(actionTypeBuf.toString()) == 0 && successValue2.compareTo
                                (actionValueBuf.toString()) == 0) {
                            returnValue = 1;
                        }
                    }

                }

                results.add(new Tuple2<String, Integer>(key, returnValue));

                return results.iterator();
            }
        });

        ccRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println("key1=" + tuple2._1() + " value1=" + tuple2._2());
            }
        });

        sc.close();
    }
}
