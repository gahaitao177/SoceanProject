package com.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/5/3.
  */
object groupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))

    val rdd = sc.parallelize(arr)

    val groupByKeyRDD = rdd.groupByKey()

    groupByKeyRDD.foreach(println(_))

    sc.stop()
  }
}
