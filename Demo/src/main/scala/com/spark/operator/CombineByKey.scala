package com.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/5/2.
  */
object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CombineByKey")
    val sc = new SparkContext(conf)

    val people = List(("male", "Mobin"), ("male", "Kpop"), ("female", "Lucy"), ("male", "Lufei"), ("female", "Amy"))

    val rdd = sc.parallelize(people)

    val combineByKeyRDD = rdd.combineByKey(
      (x: String) => (List(x), 1),
      (peo: (List[String], Int), x: String) => (x :: peo._1, peo._2 + 1),
      (sex1: (List[String], Int), sex2: (List[String], Int)) => (sex1._1 ::: sex2._1, sex1._2 + sex2._2)
    )

    combineByKeyRDD.foreach(println(_))

    sc.stop()
  }
}
