package com.youyu.hbase.streaming

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * http://www.cnblogs.com/cssdongl/p/6238007.html
  * Created by root on 2017/6/15.
  */
object SparkReadAndWriteHbase2 {
  def main(args: Array[String]): Unit = {
    val tableName = "user_data"

    val sparkConf = new SparkConf().setAppName("SparkReadAndWriteHbase").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "gs-yy-slave1,gs-yy-slave2,gs-yy-slave3")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val scan = new Scan()
    scan.setCaching(100)

    val hbaseContext = new HBaseContext(sc,conf)

    sc.stop()
  }
}
