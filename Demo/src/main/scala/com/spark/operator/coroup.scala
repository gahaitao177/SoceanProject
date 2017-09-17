package com.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/5/3.
  */
object coroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("coroupOperator")
    val sc = new SparkContext(conf)

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd1 = sc.parallelize(arr, 3)
    val rdd2 = sc.parallelize(arr1, 3)

    val groupByKeyRDD = rdd1.cogroup(rdd2)
    groupByKeyRDD.foreach(println(_))

    sc.stop()
  }
}
