package com.youyu.mllib.sxt

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/9/11.
  */
object ClassificationDecisionTree {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ClassificationDecisionTree").setMaster("local[2]")
    val sc = new SparkContext(config = conf)

    val data = MLUtils.loadLibSVMFile(sc, "data/汽车数据样本.txt")

    val splits = data.randomSplit(Array(0.7, 0.3))

    val (trainingData, testingData) = (splits(0), splits(1))
    //指明类别
    val numClasses = 2
    //指定离散变量，未指明的都当作离散变量处理
    val categoricalFeaturesInfo = Map[Int, Int](0 -> 4, 1 -> 4, 2 -> 3, 3 -> 3)
    //设定评判标准
    val impurity = "entropy"
    //数的最大深度
    val maxDepth = 5
    //设置离散化程度
    val maxBins = 32
    //生成模型
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth,
      maxBins)

    //测试
    val labelAndPreds = testingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testingData.count()

    println("Test Error = " + testErr)
    println("Learned classification tree model :\n" + model.toDebugString)

    sc.stop()
  }
}
