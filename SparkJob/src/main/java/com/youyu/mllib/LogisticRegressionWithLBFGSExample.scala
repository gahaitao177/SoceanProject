package com.youyu.mllib

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/9/8.
  */
object LogisticRegressionWithLBFGSExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionWithLBFGSExample").setMaster("local[2]")

    val sc = new SparkContext(config = conf)

    val data = MLUtils.loadLibSVMFile(sc, "sample_libsvm_data.txt")

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)

    val training = splits(0).cache()
    val testing = splits(1)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(training)

    val predictionAndLabels = testing.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val metrics = new MulticlassMetrics(predictionAndLabels)

    println("metrics.fMeasure", metrics.fMeasure)
    println("metrics.labels", metrics.labels)
    println("metrics.precision", metrics.precision)
    println("metrics.recall", metrics.recall)

    println(s"$metrics")

    model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")

    val sameModel = LogisticRegressionModel.load(sc,
      "target/tmp/scalaLogisticRegressionWithLBFGSModel")

    sc.stop()
  }
}
