package com.youyu.mllib.sxt

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.feature.{PCA, PCAModel, StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017/9/13.
  */

class Stock(scalerModel: StandardScalerModel, logisticModel: LogisticRegressionModel, randomForestModel:
RandomForestModel, pcaModel: PCAModel) extends scala.Serializable {

  /**
    * 存储股票模型
    *
    * @param sc
    * @param path
    */
  def save(sc: SparkContext, path: String): Unit = {
    this.logisticModel.save(sc, path + "逻辑回归")
    this.randomForestModel.save(sc, path + "随机森林")
    sc.parallelize(Seq(this.scalerModel)).saveAsObjectFile(path + "归一因子")
    sc.parallelize(Seq(this.pcaModel)).saveAsObjectFile(path + "pca模型")
  }


  /**
    * 计算正确率和召回率
    *
    * @param inputData
    * @param threshold
    * @return
    */
  def calRightAndRecallRate(inputData: RDD[LabeledPoint], threshold: Double): (Array[(Double, Double)], Array[
    (Double, Double)]) = {
    val newLabelData = inputData.map(p => p.copy(label = 2 * p.label - 1))

    val result = newLabelData.map { p =>
      val predicate = this.predicate(p.features, threshold)
      val result = predicate == p.label match {
        case true => 1;
        case false => 0
      }

      //预测类别，是否正确
      (predicate, result)
    }.filter(_._1 != 0).aggregateByKey((0, 0))((u, value) => (u._1 + value, u._2 + 1), (u1, u2) => (u1._1 + u2._1, u1
      ._2 + u2._2))

    val rightRate = result.mapValues(p => (p._1.toDouble / p._2, p._1, p._2)).collect()
    rightRate.foreach { case (label, rate) => println("类别\t" + label + "\t正确率为：\t" + rate) }

    val recallRate = new Array[(Double, Double)](rightRate.size)
    for (i <- 0 until rightRate.size) {
      val count = newLabelData.map(_.label == rightRate.apply(i)._1).count()
      val recall = rightRate(i)._2._1.toDouble / count
      recallRate(i) = (rightRate.apply(i)._1, recall)
    }
    (rightRate.map(p => (p._1, p._2._1)), recallRate)
  }

  /**
    * 对输入向量的类别进行预测
    *
    * @param vector
    * @param threshold
    * * @return
    */
  def predicate(vector: org.apache.spark.mllib.linalg.Vector, threshold: Double): Double = {
    val lp = scalerModel.transform(vector.toDense)
    val feature = this.pcaModel.transform(lp)
    val result1 = this.logisticModel.predict(feature) match {
      case x if x > threshold => 1;
      case x if x < 1 - threshold => (-1)
      case _ => 0
    }

    val result2 = this.randomForestModel.predict(feature) match {
      case x if x > threshold => 1;
      case x if x < 1 - threshold => (-1)
      case _ => 0
    }

    if (result2 == result1) {
      result1
    } else {
      0
    }
  }

}

object Stock {

  /**
    * 获得归一化模型
    *
    * @param inputData
    * @return
    */
  def getStandardScaler(inputData: RDD[LabeledPoint]) = {
    val vectors = inputData.map(_.features)
    println("获得归一化模型中的vertots:")
    /*vectors.foreach(v => {
      println(v.numActives + "-" +v.size)
      println(v.toJson)
    })*/
    val scalerModel = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    scalerModel
  }

  /**
    * 对RDD中的数据进行重复
    *
    * @param input
    * @param num
    * @return
    */
  def copyData(input: RDD[LabeledPoint], num: Int): RDD[LabeledPoint] = {
    input.flatMap(List.fill(num)(_))
  }

  /**
    * 对样本中的两类数据进行均衡
    *
    * @param normalInputData
    * @return
    */
  def balanceData(normalInputData: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val label1Data = normalInputData.filter(_.label == 1).cache()
    val label0Data = normalInputData.filter(_.label == 0).cache()

    val count1 = label1Data.count().toDouble
    val count0 = label0Data.count().toDouble

    val (data1, data0) = count1 > count0 match {
      case true =>
        val rate = (5 * count1 / count0).toInt
        (copyData(label1Data, 5), copyData(label0Data, rate))
      case false =>
        val rate = (5 * count0 / count1).toInt
        (copyData(label1Data, rate), copyData(label0Data, 5))
    }

    label1Data.unpersist()
    label0Data.unpersist()
    data1.union(data0)
  }

  /**
    * 读取模型
    *
    * @param sc
    * @param path
    * @return
    */
  def load(sc: SparkContext, path: String): Stock = {
    val modelLogistic = LogisticRegressionModel.load(sc, path + "逻辑回归")
    val modelRandomForest = RandomForestModel.load(sc, path + "随机森林")
    val scalerModel = sc.objectFile[StandardScalerModel](path + "回一化因子").collect()(0)
    val pcaModel = sc.objectFile[PCAModel](path + "pca模型").collect()(0)
    new Stock(scalerModel, modelLogistic, modelRandomForest, pcaModel)
  }

  /**
    * 对输入的数据进行压缩
    *
    * @param normalInputData
    * @return
    */
  def compressFeature(normalInputData: RDD[LabeledPoint]): (RDD[LabeledPoint], PCAModel) = {
    val standardData = normalInputData.map(_.features)
    val pca = new PCA(5).fit(standardData)
    val pcaData = normalInputData.map(p => p.copy(features = pca.transform(p.features)))
    (pcaData, pca)
  }

  /**
    * 训练逻辑回归模型
    *
    * @param normalInputData
    * @return
    */
  def trainLogistic(normalInputData: RDD[LabeledPoint]): LogisticRegressionModel = {
    val lr = new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    lr.optimizer.setRegParam(0.4)
    lr.optimizer.setUpdater(new SquaredL2Updater())

    val modelLogistic = lr.run(normalInputData).clearThreshold()
    modelLogistic.asInstanceOf[LogisticRegressionModel]
  }

  def trainRandomForest(normalInputData: RDD[LabeledPoint]): RandomForestModel = {
    val categoricalFeaturesInfo = Map[Int, Int]()

    //树的个数
    val numTrees = 3
    //特征子集采样策略，auto表示算法自主选取
    val featureSubsetStrategy = "auto"
    //纯度计算
    val impurity = "variance"
    //树的最大层次
    val maxDepth = 2
    //特征最大装箱数
    val maxBins = 32
    //训练随机森林回归模型

    val model = RandomForest.trainRegressor(normalInputData, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins)
    model
  }

  /**
    * 用输入数据进行训练
    *
    * @param inputData
    * @return
    */
  def run(inputData: RDD[LabeledPoint]): Stock = {
    //获得归一化模型
    val scalerModel = getStandardScaler(inputData)

    //println("scalerModel.mean" + scalerModel.mean + "scalerModel.std" + scalerModel.std)

    val normalInputData = inputData.map { point =>
      //println(point.label + "-" + point.features)
      point.copy(features = scalerModel.transform(point.features.toDense))
    }.cache()

    normalInputData.foreach(l => {
      println(l.label + "==" + l.features)
    })

    val balance = balanceData(normalInputData)

    val (newLabelFeatures, pcaModel) = compressFeature(balance)

    val modelLogistic = trainLogistic(newLabelFeatures)
    val modelRandomForest = trainRandomForest(newLabelFeatures)

    new Stock(scalerModel, modelLogistic, modelRandomForest, pcaModel)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stock").setMaster("local[2]")
    val sc = new SparkContext(config = conf)

    val inputData = MLUtils.loadLibSVMFile(sc, "data/002089特征.txt")
    val stockModel = Stock.run(inputData)
    stockModel.save(sc, "data/002089模型")
    //val model2 = Stock.load(sc, "data/002089模型")

    val (rightRate, recallRate) = stockModel.calRightAndRecallRate(inputData, 0.5)

    println(rightRate.map(p => "类别\t" + p._1 + "\t正确率为\t" + p._2).mkString("\n"))

    println(recallRate.map(p => "类别\t" + p._1 + "\t召回率\t" + p._2).mkString("\n"))

    sc.stop()
  }
}
