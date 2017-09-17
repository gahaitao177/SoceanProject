package com.youyu.spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.youyu.bigdata.mobiledata.HbaseUtils
import com.youyu.conf.ConfigurationManager
import com.youyu.constant.Constants
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

/**
  * Created by root on 2017/6/22.
  */
object TestFinalParam {
  def main(args: Array[String]): Unit = {
    //设置切片时间长度，如果提交时不指定切片长度，默认是5秒钟
    val slices = if (args.length > 0) args(0).toLong else 5L

    val kafka_list_addr = ConfigurationManager.getProperty(Constants.KAFKA_LIST)

    val sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local[2]")

    val scc = new StreamingContext(sparkConf, Durations.seconds(slices))

    val topic = Set("kafka_010_topic")

    val kafkaParam = Map("metadata.broker.list" -> kafka_list_addr, "group.id" -> Constants.KAFKA_GROUP_ID)

    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topic)

    val jsonDStream = stream.flatMap(line => {
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    //小时，天级别用户启动次数统计
    calculateStarts(jsonDStream, scc)

    scc.start()
    scc.awaitTermination()
  }


  /**
    * 实时统计小时、天级别用户启动次数
    *
    * @param reports
    * @param scc
    */

  def calculateStarts(reports: DStream[JSONObject], scc: StreamingContext) = {
    val appDataDaily = ConfigurationManager.getProperty(Constants.HBASE_APP_DATA_DAILY)
    val dailyDataTable = scc.sparkContext.broadcast(appDataDaily)

    val activeUserMap = mutable.Map[String, mutable.Map[String, Int]]()
    activeUserMap += ("2017-05-24" -> mutable.Map("key1" -> 1, "key2" -> 2))

    reports.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {

        val connection = HbaseUtils.getHbaseConn

        partitionOfRecords.foreach(x => {

          val startsTable = connection.getTable(TableName.valueOf(dailyDataTable.value))

          val reportDate = x.getString("reportTime").substring(0, 10)

          val appKey = x.getString("appKey")
          val pkg = x.getString("pkgId")
          val version = x.getString("appVersion")
          val channel = x.getString("appChannel")
          val DataType = "start_times"


          val starts = x.getJSONArray("starts")

          try {

            val flag = activeUserMap.contains(reportDate)
            import scala.collection.JavaConversions._

            if (!flag) {
              val valueMap = mutable.Map[String, Int]()

              valueMap += ("key1" -> 1)
              valueMap += ("key11" -> 2)

              activeUserMap += (reportDate -> valueMap)

              for (entry <- activeUserMap.entrySet) {
                System.out.println("Key1 = " + entry.getKey + ", Value1 = " + entry.getValue)
              }

            } else {
              val valueMap = activeUserMap(reportDate)
              val isNotExist = valueMap.contains("key3")
              if (!isNotExist) {
                valueMap += ("key3" -> 3)
              }
            }

            for (entry <- activeUserMap.entrySet) {
              System.out.println("Key2 = " + entry.getKey + ", Value2 = " + entry.getValue)
            }

            for (i <- 0 until starts.toArray().length) {
              val start: JSONObject = starts.getJSONObject(i)
              val startTime = start.getString("time")
              val startDate = startTime.substring(0, 10)
              val startHour = startTime.substring(11, 13)

              if (startDate.equals(reportDate)) {
                val startsRowKey = appKey + "#" + DataType + "#" + reportDate + "#" + pkg + "#" + version + "#" + channel
                HbaseUtils.incrementColumnValues(startsTable, startsRowKey, "data", startHour, 1)
                HbaseUtils.incrementColumnValues(startsTable, startsRowKey, "data", "all", 1)
              }
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            startsTable.close()
          }

        })
      })
    })

  }

  /**
    * 创建sparkStreaming输入流
    *
    * @param scc
    * @param kafkaParam
    * @param topics
    * @return
    */
  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
}
