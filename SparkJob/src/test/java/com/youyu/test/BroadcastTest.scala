package com.youyu.sparkStreaming

import com.alibaba.fastjson.{JSON, JSONObject}
import com.youyu.bigdata.mobiledata.HbaseUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

/**
  * Created by root on 2017/5/15.
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val slices = if (args.length > 0) args(0).toLong else 5L

    val new_user_info_day = "tmp_app_new_user"
    val new_user_stat_day = "tmp_app_data_daily"
    val user_pkg_dic = "tmp_pkg_dic"
    val active_user_stat_day = "tmp_app_active_user"

    val sparkCof = new SparkConf().setAppName("AppKafkaSparkStats").setMaster("local[2]")
    val scc = new StreamingContext(sparkCof, Durations.seconds(slices))

    val topic = Set("youyu_mobile_data_after_etl")

    val kafkaParam = Map("metadata.broker.list" -> "192.168.1.71:9164,192.168.1.73:9312,192.168.3.37:9383", "group.id"
      -> "app_youyu_data_stats_ght"
    )

    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topic)

    val jsonDStream = stream.flatMap(line => {
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    //小时，天级别活跃用户的统计
    activeUserCount(jsonDStream, scc)

    scc.start()
    scc.awaitTermination()
  }

  /**
    * 实时统计小时、天级别活跃用户的数量
    *
    * @param reports
    * @param scc
    */
  def activeUserCount(reports: DStream[JSONObject], scc: StreamingContext) = {

    println("***********************************")

    val active_user_stat_day = "tmp_app_data_daily"
    val broadcastActiveUserStat = scc.sparkContext.broadcast(active_user_stat_day)

    val activeUserMap = mutable.Map[String, mutable.Map[String, Int]]()
    val broadcastActiveUserMap = scc.sparkContext.broadcast(activeUserMap)
    broadcastActiveUserMap.value += ("2017-05-24" -> mutable.Map("key1" -> 1))

    import scala.collection.JavaConversions._

    reports.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        //获取hbase连接connection
        val connection = HbaseUtils.getHbaseConn

        partitionOfRecords.foreach(x => {
          val userStatTable = connection.getTable(TableName.valueOf(broadcastActiveUserStat.value))

          val reportTime = x.getString("reportTime").substring(0, 10)
          val hourCode = x.getString("reportTime").substring(11, 13)
          val appKey = x.getString("appKey")
          val pkg = x.getString("pkgId")
          val version = x.getString("appVersion")
          val channel = x.getString("appChannel")
          val clientMd5 = x.getString("clientIdMd5")
          val activeDataType = "active_user"

          val userStatRowKey = appKey + "#" + activeDataType + "#" + reportTime + "#" + pkg + "#" + version + "#" + channel
          val userInfoRowKey = appKey + "#" + pkg + "#" + clientMd5

          //判断当前时间Key是否在广播变量中
          val flag = broadcastActiveUserMap.value.contains(reportTime)
          if (!flag) {
            //不存在当前时间key
            //当前用户作为当前时间段内的活跃用户+1
            HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", hourCode, 1L)

            val valueMap = mutable.Map[String, Int]()
            valueMap += (userInfoRowKey -> 1)

            broadcastActiveUserMap.value += (reportTime -> valueMap)

            for (entry <- activeUserMap.entrySet) {
              System.out.println("Key = 2-1 " + entry.getKey + ", Value = 2-1 " + entry.getValue)
            }

          } else {
            //存在当前时间key
            //获取当前时间key对应的value值Map
            val valueMap = broadcastActiveUserMap.value(reportTime)
            val isNotExistKey = valueMap.contains(userInfoRowKey)
            if (!isNotExistKey) {
              //当前用户作为当前时间段内的活跃用户+1
              HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", hourCode, 1L)

              //将该用户的唯一标识加到内存中
              valueMap += (userInfoRowKey -> 1)
              broadcastActiveUserMap.value += (reportTime -> valueMap)
            }

            for (entry <- activeUserMap.entrySet) {
              System.out.println("Key = 2-2 " + entry.getKey + ", Value = 2-2 " + entry.getValue)
            }
          }

        })

        for (entry <- activeUserMap.entrySet) {
          System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
        }

      })
    })
  }

  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
}
