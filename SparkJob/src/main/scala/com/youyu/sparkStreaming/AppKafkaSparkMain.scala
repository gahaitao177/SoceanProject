package com.youyu.sparkStreaming

import com.alibaba.fastjson.{JSON, JSONObject}
import com.youyu.bigdata.mobiledata.HbaseUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2017/5/15.
  */
object AppKafkaSparkMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("AppKafkaSparkMain")
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5000))
    ssc.checkpoint(".")

    val topic = Set("youyu_mobile_data_after_etl")

    val kafkaParam = Map("metadata.broker.list" -> "192.168.1.71:9164,192.168.1.73:9312,192.168.1.88:9460")

    val stream: InputDStream[(String, String)] = createStream(ssc, kafkaParam, topic)

    val reports = stream.flatMap(line => {
      val data = JSON.parseObject(line._2)
      Some(data)
    })


    ssc.start()
    ssc.awaitTermination()
  }

  def sumNumByHour(reports: DStream[JSONObject]) = {
    val report_num = reports.map(x => (x.getString("reportTime").substring(0, 13), 1)).reduceByKey(_ + _)
    report_num.foreachRDD(rdd => {
      rdd.foreachPartition(partitioinOfRecords => {
        val connection = HbaseUtils.getHbaseConn
        partitioinOfRecords.foreach(pair => {
          val hour = pair._1
          val num = pair._2

          val tableName = TableName.valueOf("test_mobile_data_xiaxc")
          val t = connection.getTable(tableName)
          try {
            t.incrementColumnValue(hour.getBytes, "data".getBytes, "num".getBytes, num)
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            t.close()
          }

        })

      })
    })
  }

  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
}
