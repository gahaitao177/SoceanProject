package com.youyu.hdfs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * http://www.chinahadoop.cn/group/3/thread/1748
 * Created by Socean on 2017/6/13.
 */
public class SparkReadHdsf {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        args = new String[]{"hdfs://192.168.1.46/user/gaoht/data", "hdfs://192.168.1.46/user/gaoht/out"};

        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        String inputSparkFile = args[0];
        String outputSparkFile = args[1];

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkWordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(inputSparkFile, 1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        counts.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> tup2) throws Exception {
                System.out.println("*******" + tup2._1().toUpperCase() + ":" + tup2._2);
                return tup2._1().toUpperCase() + ":" + tup2._2;
            }
        }).saveAsTextFile(outputSparkFile);

        jsc.stop();
    }

}
