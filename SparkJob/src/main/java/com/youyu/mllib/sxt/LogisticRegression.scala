package com.youyu.mllib.sxt

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 逻辑回归
  * Created by root on 2017/9/11.
  */
object LogisticRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegression").setMaster("local[2]")

    val sc = new SparkContext(config = conf)

    val inputData = MLUtils.loadLibSVMFile(sc, "data/健康状况训练集.txt")
      //val inputData = MLUtils.loadLibSVMFile(sc,"data/线性不可分数据集.txt")
      //val inputData = MLUtils.loadLibSVMFile(sc, "data/w0测试数据.txt")
      .map { labelpoint =>
      val label = labelpoint.label
      val feature = labelpoint.features
      val array = Array(feature(0), feature(1), feature(0) * feature(1))
      val convertFeature = Vectors.dense(array)
      new LabeledPoint(label, convertFeature)
    }

    val splits = inputData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testingData) = (splits(0), splits(1))

    //val lr = new LogisticRegressionWithLBFGS()
    val lr = new LogisticRegressionWithSGD()

    lr.optimizer.setMiniBatchFraction(0.3)
    //设置有无w0
    lr.setIntercept(true)
    lr.optimizer.setUpdater(new L1Updater)
    lr.optimizer.setUpdater(new SquaredL2Updater)
    lr.optimizer.setRegParam(0.1)

    val model = lr.run(trainingData)

    val result = testingData.map { point => Math.abs(point.label - model.predict(point.features)) }

    println("正确率=" + (1.0 - result.mean()))

    println(model.weights.toArray.mkString(" "))

    println(model.intercept)

    /*val result = testingData.foreach { p =>
      val score = model.clearThreshold().predict(p.features)

      println(score)
    }

    println("阈值为" + model.getThreshold)*/

    sc.stop()
  }
}
