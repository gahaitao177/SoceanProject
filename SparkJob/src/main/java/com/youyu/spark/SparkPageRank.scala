package com.youyu.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by root on 2017/6/15.
  */
object SparkPageRank {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val links = sc.parallelize(List(("A", List("B", "C")), ("B", List("A", "C")), ("C", List("A", "B", "D")), ("D", List("C")))).partitionBy(new HashPartitioner(100)).persist()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 0 until 20) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }

      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }

    val list = ranks.sortByKey().collect()

    for (r <- list) {
      System.out.println(r)
    }

    sc.stop()
  }
}
