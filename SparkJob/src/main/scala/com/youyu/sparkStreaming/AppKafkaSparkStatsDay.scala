package com.youyu.sparkStreaming

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.youyu.bigdata.mobiledata.HbaseUtils
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{DataTypes, StringType, StructField}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

/**
  * Created by root on 2017/5/15.
  */
object AppKafkaSparkStatsDay {
  def main(args: Array[String]): Unit = {

    val topics= "test"

    val slices = if (args.length > 0) args(0).toLong else 5L

    val new_user_info_day = "tmp_app_new_user"
    val new_user_stat_day = "tmp_app_data_daily"
    val user_pkg_dic = "tmp_pkg_dic"
    val active_user_stat_day = "tmp_app_active_user"

    val sparkConf = new SparkConf().setAppName("AppKafkaSparkStats")
    sparkConf.set("spark.default.parallelism", "100")

    val scc = new StreamingContext(sparkConf, Durations.seconds(slices))

    val activeUserMap = mutable.Map[String, mutable.Map[String, String]]()
    val broadcastActiveUserMap = scc.sparkContext.broadcast(activeUserMap)

    //获取用户信息表中的RowKey
    val userInfoRowKeyMap = HbaseUtils.getAllRowKey(new_user_info_day)
    val broadcastUserInfoMap = scc.sparkContext.broadcast(userInfoRowKeyMap)

    //获取用户信息字典表中的RowKey
    val userPkgDictRowKey = HbaseUtils.getAllRowKey(user_pkg_dic)
    val broadcastUserDictMap = scc.sparkContext.broadcast(userPkgDictRowKey)

    val broadcastUserInfo = scc.sparkContext.broadcast(new_user_info_day)
    val broadcastUserStat = scc.sparkContext.broadcast(new_user_stat_day)
    val broadcastUserDict = scc.sparkContext.broadcast(user_pkg_dic)
    val broadcastActiveUserInfo = scc.sparkContext.broadcast(active_user_stat_day)

    val topic = Set("youyu_mobile_data_after_etl")

    val kafkaParam = Map("metadata.broker.list" -> "192.168.1.71:9164,192.168.1.73:9312,192.168.3.37:9383", "group.id"
      -> "app_youyu_data_stats"
    )

    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topic)

    val jsonDStream = stream.flatMap(line => {
      val data = JSON.parseObject(line._2)
      Some(data)
    })


    //用户字典数据维护
    userInfoPkgDict(jsonDStream, broadcastUserDictMap, broadcastUserDict)

    //小时，天级别新用户的统计
    newUserCount(jsonDStream, broadcastUserInfoMap, broadcastUserInfo, broadcastUserStat)

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
    val date: Date = new Date

    //获取当前时间
    val currentTime = DateUtils.getLastDay(date, 0)
    //获取当前时间前两天的时间
    val lastTwoDayTime = DateUtils.getLastDay(date, -2).substring(0, 10)

    //获取当前时间所属的小时
    val currentHour = DateUtils.splitDate(currentTime)("hour")

    val active_user_stat_day = "tmp_app_data_daily"
    val broadcastActiveUserStat = scc.sparkContext.broadcast(active_user_stat_day)

    val activeUserMap = mutable.Map[String, mutable.Map[String, Int]]()
    val broadcastActiveUserMap = scc.sparkContext.broadcast(activeUserMap)

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
          }

        })

        if ("00" == currentHour) {
          broadcastActiveUserMap.value.remove(lastTwoDayTime)
          println("-------------------------" + currentHour + "===" + lastTwoDayTime)
        }
      })
    })
  }


  /**
    * 实时统计小时、天级别活跃用户的数量
    *
    * @param reports
    * @param scc
    */
  def activeUserCountBySQL(reports: DStream[JSONObject], scc: StreamingContext) = {
    val active_user_stat = "tmp_app_data_daily"

    val broadcastActiveUserStat = scc.sparkContext.broadcast(active_user_stat)

    reports.foreachRDD(rdd => {
      val sqlContext = new SQLContext(rdd.context)

      val appActiveUserRowRDD = rdd.flatMap(x => {
        val time = x.getString("reportTime").substring(0, 13)
        val pkg = x.getString("pkgId")
        val key = x.getString("appKey")
        val version = x.getString("appVersion")
        val channel = x.getString("appChannel")
        val clientId = x.getString("clientId")
        val clientMd5 = x.getString("clientIdMd5")

        val row = RowFactory.create(time, pkg, key, version, channel, clientId, clientMd5)

        Some(row)
      })

      val fields = Array(StructField("time", StringType, true),
        StructField("pkg", StringType, true),
        StructField("key", StringType, true),
        StructField("version", StringType, true),
        StructField("channel", StringType, true),
        StructField("clientId", StringType, true),
        StructField("clientMd5", StringType, true))

      val schema = DataTypes.createStructType(fields)

      val activeUserDF = sqlContext.createDataFrame(appActiveUserRowRDD, schema)

      activeUserDF.registerTempTable("active_user")

      val activeUserCountDF = sqlContext.sql("select " +
        "time," +
        "(case when pkg = 'null' or pkg = '' then 'null' else pkg end) pkg," +
        "(case when key = 'null' or key = '' then 'null' else key end) key," +
        "(case when version = 'null' or version = '' then 'null' else version end) version," +
        "(case when channel = 'null' or channel = '' then 'null' else channel end) channel," +
        "count(1) " +
        "from active_user " +
        "group by time,pkg,key,version,channel ")

      println("**************************")
      activeUserCountDF.show()
      println("**************************")

      activeUserCountDF.foreachPartition(row => {
        val connection = HbaseUtils.getHbaseConn
        row.foreach(x => {
          val activeUserStatTable = connection.getTable(TableName.valueOf(broadcastActiveUserStat.value))

          val reportTime = x.getString(0).substring(0, 10)
          val hourCode = x.getString(0).substring(11, 13)
          val pkg = x.getString(1)
          val appKey = x.getString(2)
          val version = x.getString(3)
          val channel = x.getString(4)
          val count = x.getLong(5)
          val activeDataType = "active_user"

          val userStatRowKey = appKey + "#" + activeDataType + "#" + reportTime + "#" + pkg + "#" + version + "#" + channel

          try {
            val columns = Array(hourCode)
            val values = Array(count.toString)

            HbaseUtils.addRow1(activeUserStatTable, userStatRowKey, "data", columns, values)

          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            activeUserStatTable.close()
          }

        })
      })

    })
  }

  /**
    * 实时更新用户字典表中的信息
    *
    * @param reports
    * @param broadcastMap
    * @param broadcastUserDict
    */
  def userInfoPkgDict(reports: DStream[JSONObject], broadcastMap: Broadcast[mutable.Map[String, Integer]], broadcastUserDict: Broadcast[String]) = {
    reports.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = HbaseUtils.getHbaseConn

        partitionOfRecords.foreach(x => {
          val userDictTable = connection.getTable(TableName.valueOf(broadcastUserDict.value))

          val clientId = x.getString("clientId")
          val appName = x.getString("appName")
          val platform = x.getString("deviceType")

          val userDictRowKey = if (clientId.length == 0) "null" else clientId

          try {
            val flag = broadcastMap.value.contains(userDictRowKey)

            if (!flag) {
              val isExistKey = HbaseUtils.isNotExistKey(userDictTable, userDictRowKey)

              if (isExistKey) {
                HbaseUtils.addRow(userDictTable, userDictRowKey, "info", "app_name", appName)
                HbaseUtils.addRow(userDictTable, userDictRowKey, "info", "app_platform", platform)

                broadcastMap.value += (userDictRowKey -> 1)
              } else {
                broadcastMap.value += (userDictRowKey -> 1)
              }
            }
          }
          catch {
            case e: Exception =>
              e.printStackTrace()
          }
          finally {
            userDictTable.close()
          }
        })
      })
    })
  }

  /**
    * 实时统计小时、天级别新用户
    *
    * @param reports
    * @param broadcastMap
    * @param broadcastUserInfo
    * @param broadcastUserStat
    */
  def newUserCount(reports: DStream[JSONObject], broadcastMap: Broadcast[mutable.Map[String, Integer]], broadcastUserInfo: Broadcast[String], broadcastUserStat: Broadcast[String]) = {
    reports.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        //获取hbase连接connection
        val connection = HbaseUtils.getHbaseConn

        partitionOfRecords.foreach(x => {
          val userInfoTable = connection.getTable(TableName.valueOf(broadcastUserInfo.value))
          val userStatTable = connection.getTable(TableName.valueOf(broadcastUserStat.value))

          val reportTime = x.getString("reportTime").substring(0, 10)
          val hourCode = x.getString("reportTime").substring(11, 13)

          val appKey = x.getString("appKey")
          val pkg = x.getString("pkgId")
          val version = x.getString("appVersion")
          val channel = x.getString("appChannel")
          val clientId = x.getString("clientId")
          val clientMd5 = x.getString("clientIdMd5")
          val activeDataType = "new_user"

          val userStatRowKey = appKey + "#" + activeDataType + "#" + reportTime + "#" + pkg + "#" + version + "#" + channel
          val userInfoRowKey = appKey + "#" + pkg + "#" + clientMd5

          try {
            //判断当前rowKey在内存中是否存在，如果存在不做任何操作，如果不存在执行下面操作
            val flag = broadcastMap.value.contains(userInfoRowKey)

            if (!flag) {
              //如果当前的rowKey在内存中不存在，则在操作表之前判断当前rowkey是否存在当前表中，如果不存在，则执行下面操作
              val isExistKey = HbaseUtils.isNotExistKey(userInfoTable, userInfoRowKey)

              if (isExistKey) {
                //将信息插入到用户信息表中
                HbaseUtils.addRow(userInfoTable, userInfoRowKey, "info", "client_id", clientId)
                HbaseUtils.addRow(userInfoTable, userInfoRowKey, "info", "reg_date", reportTime)
                HbaseUtils.addRow(userInfoTable, userInfoRowKey, "info", "app_version", version)
                HbaseUtils.addRow(userInfoTable, userInfoRowKey, "info", "app_channel", channel)

                //对统计新用户表中新用户的数量增加1
                HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", "all", 1L)
                HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", hourCode, 1L)

                //更新内存中不存在的该rowKey
                broadcastMap.value += (userInfoRowKey -> 1)
              } else {
                //更新内存中不存在的该rowKey
                broadcastMap.value += (userInfoRowKey -> 1)
              }
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            userInfoTable.close()
            userStatTable.close()
          }
        })
      })
    })
  }

  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
}
