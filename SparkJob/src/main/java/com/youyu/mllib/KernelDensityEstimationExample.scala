package com.youyu.mllib

import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/9/8.
  */
object KernelDensityEstimationExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KernelDensityEstimationExample").setMaster("local[2]")

    val sc = new SparkContext(config = conf)

    val data: RDD[Double] = sc.parallelize(Seq(1, 1, 1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 9))

    val kd = new KernelDensity().setSample(data).setBandwidth(3.0)

    val densities = kd.estimate(Array(-1.0, 2.0, 5.0))

    densities.foreach(println)

    sc.stop()
  }
}
