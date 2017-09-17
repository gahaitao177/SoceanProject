package com.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/5/2.
  */
object MapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MapValue")
    val sc = new SparkContext(conf)

    val list = List(("scala", 22), ("python", 20), ("java", 23))

    val rdd = sc.parallelize(list)

    val mapValuesRDD = rdd.mapValues(_ + 2)

    mapValuesRDD.foreach(println(_))

    sc.stop()
  }
}
