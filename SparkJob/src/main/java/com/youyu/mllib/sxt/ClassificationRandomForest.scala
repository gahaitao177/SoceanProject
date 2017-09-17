package com.youyu.mllib.sxt

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 随机森林
  * Created by root on 2017/9/12.
  */
object ClassificationRandomForest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ClassificationRandomForest").setMaster("local[2]")
    val sc = new SparkContext(config = conf)

    //读取数据
    val data = MLUtils.loadLibSVMFile(sc, "data/汽车数据样本.txt")
    //将样本按7:3的比例进行分成
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testingData): Tuple2[RDD[LabeledPoint], RDD[LabeledPoint]] = (splits(0), splits(1))

    //分类数
    val numClasses = 2
    //categoricalFeaturesInfo 为空，意味着所有的特征为连续型变量
    val categoricalFeaturesInfo = Map[Int, Int](0 -> 4, 1 -> 4, 2 -> 3, 3 -> 3)

    //树的个数
    val numTrees = 3
    //特征子集采样策略，auto表示算法自主选取
    val featureSubsetStrategy = "auto"
    //纯度计算
    val impurity = "entropy"
    //树的最大层次
    val maxDepth = 3
    //特征最大装箱数
    val maxBins = 32
    //训练随机森林分类器，trainClassifier 返回的事RandomForestModel 对象
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins)

    //打印模型
    println("打印模型", model.toDebugString)
    //保存模型
    model.save(sc, "data/汽车保险")

    //在测试集上进行测试
    val testErr = testingData.map { point =>
      println("point.label=" + point.label, "point.features=" + point.features)
      val prediction = model.predict(point.features)
      Math.abs(prediction - point.label)
    }.mean()

    println(1 - testErr)

    sc.stop()
  }
}
