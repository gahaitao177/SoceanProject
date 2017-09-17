package com.youyu.mllib

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/9/8.
  */
object HypothesisTestingKolmogorovSmirnovTestExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HypothesisTestingKolmogorovSmirnovTestExample").setMaster("local[2]")

    val sc = new SparkContext(config = conf)

    val data: RDD[Double] = sc.parallelize(Seq(0.1, 0.15, 0.2, 0.3, 0.25))

    val testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)

    println(testResult)
    println("---------------------")

    val myCDF = Map(0.1 -> 0.2, 0.15 -> 0.6, 0.2 -> 0.05, 0.3 -> 0.05, 0.25 -> 0.1)
    val testResult2 = Statistics.kolmogorovSmirnovTest(data, myCDF)
    println(testResult2)
    sc.stop()
  }
}
