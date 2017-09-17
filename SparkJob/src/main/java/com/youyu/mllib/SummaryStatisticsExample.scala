package com.youyu.mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Socean on 2017/9/7.
  * http://blog.csdn.net/u011926899/article/details/50421750
  */
object SummaryStatisticsExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SummaryStatisticsExample").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val obserrvations = sc.parallelize(Seq(
      Vectors.dense(1.0, 10.0, 100.0),
      Vectors.dense(0.0, 20.0, 200.0),
      Vectors.dense(0.0, 0.0, 300.0)
    ))

    val summary: MultivariateStatisticalSummary = Statistics.colStats(obserrvations)

    println(summary.mean)

    println(summary.variance)

    println(summary.numNonzeros)

    sc.stop()
  }
}
