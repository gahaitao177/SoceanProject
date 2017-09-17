package com.youyu.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2017/3/29.
 */
object SparkHBase {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkHBase").setMaster("local")

    val sc: SparkContext = new SparkContext(sparkConf)

    val conf: Configuration = HBaseConfiguration.create()

    //设置zookeeper集群地址和端口号
    //conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3")
    //conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tableName = "student"

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val dataRDD = sc.makeRDD(Array("1,jack,15", "2,Lily,16", "3,mike,16"))

    val rdd = dataRDD.map(_.split(",")).map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
      * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
      * Put.add方法接收三个参数：列族，列名，数据
      */

      val put: Put = new Put(Bytes.toBytes(arr(0).toInt))

      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("grade"), Bytes.toBytes(arr(2).toInt))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }
    }

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
