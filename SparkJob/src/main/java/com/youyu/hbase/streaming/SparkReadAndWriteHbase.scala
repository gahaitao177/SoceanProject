package com.youyu.hbase.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://github.com/caroljmcdonald/SparkStreamingHBaseExample/blob/master/src/main/scala/examples/HBaseReadWrite.scala
  * Created by root on 2017/6/15.
  */
object SparkReadAndWriteHbase {
  final val tableName = "user_data"
  final val cfStatsBytes = Bytes.toBytes("info")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkReadAndWriteHbase").setMaster("local[2]")

    val sc = new SparkContext(config = sparkConf)

    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "gs-yy-slave1,gs-yy-slave2,gs-yy-slave3")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    println(hbaseRDD.count())

    println("------------------")

    val resultRDD = hbaseRDD.map(tuple => tuple._2)

    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()).split(" ")(0), Bytes.toDouble(result.value)))

    keyValueRDD.take(10).foreach(kv => println(kv._1 + "---" + kv._2))

    val keyStatsRDD = keyValueRDD.groupByKey().mapValues(list => StatCounter(list))
    keyStatsRDD.take(5).foreach(println)

    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    keyStatsRDD.map { case (k, v) => convertToPut(k, v) }.saveAsHadoopDataset(jobConfig)

    sc.stop()
  }

  // convert rowkey, stats to put
  def convertToPut(key: String, stats: StatCounter): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(key))
    // add columns with data values to put
    p.add(cfStatsBytes, Bytes.toBytes("max"), Bytes.toBytes(stats.max))
    p.add(cfStatsBytes, Bytes.toBytes("min"), Bytes.toBytes(stats.min))
    p.add(cfStatsBytes, Bytes.toBytes("mean"), Bytes.toBytes(stats.mean))
    (new ImmutableBytesWritable, p)
  }

}
