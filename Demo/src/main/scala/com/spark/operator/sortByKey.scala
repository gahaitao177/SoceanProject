package com.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/5/3.
  */
object sortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sortByKey")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = sc.parallelize(arr)
    val sortByKeyRDD = rdd.sortByKey()

    sortByKeyRDD.foreach(println(_))
    sc.stop()
  }
}
