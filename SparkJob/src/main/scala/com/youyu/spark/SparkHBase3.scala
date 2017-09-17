package com.youyu.spark

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
 * http://blog.csdn.net/u013468917/article/details/52822074
 * 使用saveAsNewAPIHadoopDataset写入数据
 *
 * Created by root on 2017/3/29.
 */
object SparkHBase3 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseTest3").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tableName = "student"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "slave1,slave2,slave3")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val dataRDD = sc.makeRDD(Array("1,jack,15", "2,Lily,16", "3,mike,16"))
    val rdd = dataRDD.map(_.split(",")).map { arr => {
      val put: Put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name3"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age3"), Bytes.toBytes(arr(2).toInt))

      (new ImmutableBytesWritable(), put)
    }
    }

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
