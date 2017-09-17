package com.caiyi.spark.daily;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by root on 2016/12/20.
 */
public class SecondarySortTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("test");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = jsc.textFile("D://test3.txt");

        JavaPairRDD<SecondKey, Integer> pairRDD = lineRDD.mapToPair(new PairFunction<String, SecondKey, Integer>() {
            public Tuple2<SecondKey, Integer> call(String line) throws Exception {

                SecondKey key = new SecondKey(line.split("\\s+")[0], line.split("\\s+")[1]);
                Integer value = Integer.valueOf(line.split("\\s+")[2]);

                return new Tuple2<SecondKey, Integer>(key, value);
            }
        });

        JavaPairRDD<SecondKey, Iterable<Integer>> groupRDD = pairRDD.groupByKey();

        /*groupRDD.foreach(new VoidFunction<Tuple2<SecondKey, Iterable<Integer>>>() {
            public void call(Tuple2<SecondKey, Iterable<Integer>> tuple2) throws Exception {
                System.out.println("key1:" + tuple2._1().getFirst() + " key2" + tuple2._1().getSecond() + " value:" +
                        tuple2._2());
            }
        });*/

        JavaPairRDD<SecondKey,Integer> pairCountRDD = groupRDD.mapToPair(new PairFunction<Tuple2<SecondKey, Iterable<Integer>>, SecondKey, Integer>() {
            public Tuple2<SecondKey, Integer> call(Tuple2<SecondKey, Iterable<Integer>> tuple2)
                    throws Exception {

                SecondKey key = tuple2._1();
                Integer sum = 0;

                Iterator<Integer> iterator = tuple2._2().iterator();
                while (iterator.hasNext()) {
                    Integer value = iterator.next();
                    sum++;
                }

                return new Tuple2<SecondKey, Integer>(key, sum);
            }
        });

        pairCountRDD.foreach(new VoidFunction<Tuple2<SecondKey, Integer>>() {
            public void call(Tuple2<SecondKey, Integer> tuple2) throws Exception {
                System.out.println("key1:" + tuple2._1().getFirst() + " key2" + tuple2._1().getSecond() + " value:" +
                        tuple2._2());
            }
        });

        /*JavaPairRDD<SecondKey, Integer> sortPairRDD = pairRDD.sortByKey();

        sortPairRDD.foreach(new VoidFunction<Tuple2<SecondKey, Integer>>() {
            public void call(Tuple2<SecondKey, Integer> tuple2) throws Exception {

                System.out.println("key1:" + tuple2._1().getFirst() + " key2:" + tuple2._1().getSecond() + " value:" +
                        tuple2._2());
                //System.out.println("key=" + tuple2._1() + "  value=" + tuple2._2());
            }
        });*/

        jsc.close();
    }
}
