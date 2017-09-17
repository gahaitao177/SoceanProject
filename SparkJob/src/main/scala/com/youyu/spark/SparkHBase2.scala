package com.youyu.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * http://blog.csdn.net/u013468917/article/details/52822074
 * 从hbase读取数据转化成RDD
 * Created by root on 2017/3/29.
 */
//noinspection ScalaDeprecation
object SparkHBase2 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest3").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tableName = "student"

    val conf: Configuration = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println(count)

    hBaseRDD.foreach { case (_, result) => {
      //获取行健
      val key = Bytes.toString(result.getRow)

      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("info".getBytes, "name3".getBytes))
      val age = Bytes.toInt(result.getValue("cf".getBytes, "age".getBytes))

      println("Row key:" + key + " Name:" + name + " Age:" + age)
    }
    }

    sc.stop()
    admin.close()
  }
}
