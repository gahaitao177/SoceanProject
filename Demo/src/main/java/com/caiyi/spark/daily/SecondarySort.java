package com.caiyi.spark.daily;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @author Administrator
 */
public class SecondarySort {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D://sort.txt");

        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(

                new PairFunction<String, SecondarySortKey, String>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                        String[] lineSplited = line.split("\\s+");
                        SecondarySortKey key = new SecondarySortKey(
                                Integer.valueOf(lineSplited[0]),
                                Integer.valueOf(lineSplited[2]));
                        System.out.println("=============" + key.getFirst() + ":" + key.getSecond());
                        String value = lineSplited[1];
                        return new Tuple2<SecondarySortKey, String>(key, value);
                    }

                });

        JavaPairRDD<SecondarySortKey, Iterable<String>> groupRDD = pairs.groupByKey();

        groupRDD.foreach(new VoidFunction<Tuple2<SecondarySortKey, Iterable<String>>>() {
            public void call(Tuple2<SecondarySortKey, Iterable<String>> tuple2) throws
                    Exception {
                System.out.println(tuple2._1().getFirst() +":"+ tuple2._1().getSecond() + " == " + tuple2._2());
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey();

        JavaRDD<String> sortedLines = sortedPairs.map(

                new Function<Tuple2<SecondarySortKey, String>, String>() {

                    private static final long serialVersionUID = 1L;

                    public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                        return v1._2();
                    }

                });

        sortedLines.foreach(new VoidFunction<String>() {

            private static final long serialVersionUID = 1L;

            public void call(String t) throws Exception {
                System.out.println(t);
            }

        });

        sc.close();
    }

}
