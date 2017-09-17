package com.caiyi.spark.daily;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

;

/**
 * Created by root on 2016/12/21.
 */
public class aa {

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("LeftJoinOperator").setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> nameList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 2),
                new Tuple2<Integer, Integer>(3, 4),
                new Tuple2<Integer, Integer>(3, 6));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(3, 9));


        JavaPairRDD<Integer, Integer> test1RDD = jsc.parallelizePairs(nameList);
        JavaPairRDD<Integer, Integer> test2RDD = jsc.parallelizePairs(scoreList);

        test1RDD.cogroup(test2RDD).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>>() {


            public void call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> tup2)
                    throws Exception {
                System.out.println(tup2._1() + "-" + tup2._2()._1() + " -" + tup2._2()._2());
            }
        });

        jsc.close();
    }
}
