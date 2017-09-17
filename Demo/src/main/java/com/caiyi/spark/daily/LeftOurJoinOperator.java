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
public class LeftOurJoinOperator {

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("LeftJoinOperator").setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> nameList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 200),
                new Tuple2<Integer, Integer>(3, 300),
                new Tuple2<Integer, Integer>(4, 400),
                new Tuple2<Integer, Integer>(5, 500));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 11),
                new Tuple2<Integer, Integer>(2, 22),
                new Tuple2<Integer, Integer>(3, 33),
                new Tuple2<Integer, Integer>(3, 333),
                new Tuple2<Integer, Integer>(4, 44),
                new Tuple2<Integer, Integer>(5, 55),
                new Tuple2<Integer, Integer>(7, 77));

        List<Tuple2<Integer, Integer>> classList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 111),
                new Tuple2<Integer, Integer>(2, 222),
                new Tuple2<Integer, Integer>(3, 333),
                new Tuple2<Integer, Integer>(4, 444),
                new Tuple2<Integer, Integer>(5, 555),
                new Tuple2<Integer, Integer>(6, 666));

        List<Tuple2<String, Integer>> test1 = Arrays.asList(
                new Tuple2<String, Integer>("高", 1),
                new Tuple2<String, Integer>("赵", 2),
                new Tuple2<String, Integer>("唐", 3),
                new Tuple2<String, Integer>("李", 5),
                new Tuple2<String, Integer>("韦", 7));

        List<Tuple2<String, Integer>> test2 = Arrays.asList(
                new Tuple2<String, Integer>("高", 11),
                new Tuple2<String, Integer>("赵", 22),
                new Tuple2<String, Integer>("唐", 33),
                new Tuple2<String, Integer>("孙", 44),
                new Tuple2<String, Integer>("李", 55),
                new Tuple2<String, Integer>("刘", 66));

        List<Tuple2<String, Integer>> test3 = Arrays.asList(
                new Tuple2<String, Integer>("aa", 111),
                new Tuple2<String, Integer>("bb", 222),
                new Tuple2<String, Integer>("cc", 333),
                new Tuple2<String, Integer>("dd", 444),
                new Tuple2<String, Integer>("ee", 555));

        JavaPairRDD<String, Integer> test1RDD = jsc.parallelizePairs(test1);
        JavaPairRDD<String, Integer> test2RDD = jsc.parallelizePairs(test2);
        JavaPairRDD<String, Integer> test3RDD = jsc.parallelizePairs(test3);

        /*JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> tes1AndRDD = test1RDD.cogroup(test2RDD);

        tes1AndRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>>() {
            public void call(Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> tuple2) throws
                    Exception {
                System.out.println("key1=" + tuple2._1() + " value1=" + tuple2._2()._1() + " value2=" + tuple2._2()
                        ._2());
            }
        });

        JavaPairRDD<String, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> vvvRDD = test1RDD.cogroup
                (test2RDD, test3RDD);

        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> aaRDD = vvvRDD.mapToPair(new
        PairFunction<Tuple2<String, Tuple3<Iterable<Integer>, Iterable<Integer>,
                Iterable<Integer>>>, String, Tuple3<Integer, Integer, Integer>>() {

            public Tuple2<String, Tuple3<Integer, Integer, Integer>> call(Tuple2<String, Tuple3<Iterable<Integer>,
                    Iterable<Integer>, Iterable<Integer>>> tup2) throws Exception {
                Iterator ittBank = tup2._2()._1().iterator();
                Integer bankCount = 0;
                while (ittBank.hasNext()) {
                    bankCount = Integer.valueOf(String.valueOf(ittBank.next()));
                }

                Iterator ittUser = tup2._2()._2().iterator();
                Integer userCount = 0;
                while (ittUser.hasNext()) {
                    userCount = Integer.valueOf(String.valueOf(ittUser.next()));
                }

                Iterator ittBill = tup2._2()._3().iterator();
                Integer billCount = 0;
                while (ittBill.hasNext()) {
                    billCount = Integer.valueOf(String.valueOf(ittBill.next()));
                }

                return new Tuple2<String, Tuple3<Integer, Integer, Integer>>(tup2._1(), new Tuple3<Integer, Integer,
                        Integer>(bankCount, userCount, billCount));
            }
        });
*/
        /*aaRDD.foreach(new VoidFunction<Tuple2<String, Tuple3<Integer, Integer, Integer>>>() {
            public void call(Tuple2<String, Tuple3<Integer, Integer, Integer>> tuple2) throws Exception {
                System.out.println("key1=" + tuple2._1() + " value1=" + tuple2._2()._1() + " value2=" + tuple2._2()
                        ._2() + " value3=" + tuple2._2()._3());
            }
        });*/

        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> aaRDD = test1RDD.leftOuterJoin(test2RDD);

        aaRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<Integer>>>>() {
            public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> tuple2) throws Exception {

            }
        });



        /*
        JavaPairRDD<String, Tuple2<Tuple2<Integer, Optional<Integer>>, Optional<Integer>>> bbRDD = aaRDD
                .leftOuterJoin(test3RDD);

        bbRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Tuple2<Integer, Optional<Integer>>, Optional<Integer>>>>
                () {
            public void call(Tuple2<String, Tuple2<Tuple2<Integer, Optional<Integer>>, Optional<Integer>>> t) throws
                    Exception {
                System.out.println("1" +t._1() + " 2"+t._2()._1()._1() + " 3" +t._2()._1()._2() + " 4" + t._2()._2());
            }
        });*/

        JavaPairRDD<Integer, Integer> nameRDD = jsc.parallelizePairs(nameList);
        JavaPairRDD<Integer, Integer> scoreRDD = jsc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, Integer> classRDD = jsc.parallelizePairs(classList);


        JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> nameAndScoreRDD = nameRDD.leftOuterJoin(scoreRDD);

        /*nameAndScoreRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, Optional<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple2<Integer, Optional<Integer>>> tuple2) throws Exception {
                Integer count = 0;
                if (tuple2._2()._2().isPresent()) {
                    count = tuple2._2()._2().get();
                }

                System.out.println("key1=" + tuple2._1() + " value1=" + tuple2._2()._1() + " value2=" + count);
            }
        });
*/
        /*JavaPairRDD<Integer, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> cogroupRDD = nameRDD
                .cogroup(scoreRDD, classRDD);

        cogroupRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple3<Iterable<Integer>, Iterable<Integer>,
                Iterable<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>>
                                     t) throws Exception {
                Iterator<Integer> itr1 = t._2()._1().iterator();
                Iterator<Integer> itr2 = t._2()._2().iterator();
                Iterator<Integer> itr3 = t._2()._3().iterator();

                Integer sum1 = 0;
                while (itr1.hasNext()) {
                    sum1 += itr1.next();
                }

                Integer sum2 = 0;
                while (itr2.hasNext()) {
                    sum2 += itr2.next();
                }

                Integer sum3 = 0;
                while (itr3.hasNext()) {
                    sum3 += itr3.next();
                }

                System.out.println(t._1() + ":" + sum1 + ":" + sum2 + ":" + sum3);

            }
        });*/

        /*JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> fullRDD = nameRDD.fullOuterJoin(scoreRDD);

        fullRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<Integer>, Optional<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> tuple2)
                    throws Exception {
                Integer count1 = 0;
                Integer count2 = 0;
                if (tuple2._2()._1().isPresent()) {
                    count1 = tuple2._2()._1().get();
                }

                if (tuple2._2()._2().isPresent()) {
                    count2 = tuple2._2()._2().get();
                }

                System.out.println(tuple2._1() + ":" + count1 + ":" + count2);
            }
        });*/

        /*JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> nameAndScoreRDD = nameRDD.leftOuterJoin(scoreRDD)
                .filter(new Function<Tuple2<Integer, Tuple2<Integer, Optional<Integer>>>, Boolean>() {
                    public Boolean call(Tuple2<Integer, Tuple2<Integer, Optional<Integer>>> v1) throws Exception {
                        Integer aa = v1._2()._1();
                        if (aa == 5) {
                            return false;
                        }

                        return true;
                    }
                });*/

        /*nameAndScoreRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, Optional<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple2<Integer, Optional<Integer>>> tup) throws Exception {
                Integer count1 = 0;

                if (tup._2()._2().isPresent()) {
                    count1 = tup._2()._2().get();
                }

                System.out.println(tup._1() + ":" + tup._2()._1() + ":" + count1);
            }
        })*/
        ;

        jsc.close();
    }
}
