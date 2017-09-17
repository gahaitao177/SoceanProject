package com.youyu.mllib

import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Socean on 2017/9/8.
  */
object HypothesisTestingExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HypothesisTestingExample").setMaster("local[2]")

    val sc = new SparkContext(conf)

    //由事件发生概率组成
    val vec = Vectors.dense(0.1, 0.15, 0.2, 0.3, 0.25)
    //计算适配度
    val goodnessOfFitTestResult = Statistics.chiSqTest(vec)
    //汇总结果，包括p值，自由度，校验统计量，所用是的方法，以及零假设
    println(s"$goodnessOfFitTestResult\n")

    //应急矩阵
    val mat: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    //根据输入矩阵计算Person独立性校验
    val independenceTestResult = Statistics.chiSqTest(mat)
    //汇总结果
    println(s"$independenceTestResult\n")

    //键值对
    val obs: RDD[LabeledPoint] = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 2.0, 0.0)),
      LabeledPoint(-1.0, Vectors.dense(-1.0, 0.0, -0.5))
    ))

    val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)

    featureTestResults.zipWithIndex.foreach {
      case (k, v) =>
        println("Column " + (v + 1).toString + ":")
        println(k)
    }

    sc.stop()
  }
}
