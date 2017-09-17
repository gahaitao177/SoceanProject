package com.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/5/3.
  */
object foldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]") setAppName ("foldByKet")
    val sc = new SparkContext(conf)

    val people = List(("Mobin", 2), ("Mobin", 1), ("Lucy", 2), ("Amy", 1), ("Lucy", 3))
    val rdd = sc.parallelize(people)
    val foldByKeyRDD = rdd.foldByKey(2)(_ + _)

    foldByKeyRDD.foreach(println(_))
    sc.stop()
  }
}
