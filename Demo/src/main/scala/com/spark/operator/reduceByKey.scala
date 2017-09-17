package com.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/5/3.
  */
object reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("reduceByKey")
    val sc = new SparkContext(conf)

    val arr = List(("A", 3), ("A", 2), ("B", 1), ("B", 3))
    val rdd = sc.parallelize(arr)

    val reduceByKeyRDD = rdd.reduceByKey(_ + _)

    reduceByKeyRDD.foreach(println(_))
    sc.stop()
  }
}
