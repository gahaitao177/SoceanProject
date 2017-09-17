package com.youyu.sparkStreaming

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.youyu.bigdata.mobiledata.HbaseUtils
import com.youyu.conf.ConfigurationManager
import com.youyu.constant.Constants
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

/**
  * http://www.dataguru.cn/article-11031-1.html
  * Created by Socean on 2017/5/15.
  */
object AppKafkaSparkStatsTest {

  def main(args: Array[String]): Unit = {
    //设置切片时间长度，如果提交时不指定切片长度，默认是5秒钟
    val slices = if (args.length > 0) args(0).toLong else 5L

    val kafka_list_addr = ConfigurationManager.getProperty(Constants.KAFKA_LIST)

    val sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME)
    //sparkConf.set("spark.default.parallelism", "10")
    //sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    //sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true") //http://www.tuicool.com/articles/mMJNf2F
    sparkConf.set("spark.streaming.backpressure.enabled","true") //http://www.tuicool.com/articles/mMJNf2F
    sparkConf.set("spark.storage.memoryFraction","0.3") //默认是0.6

    val sparkContext = new SparkContext()
    val scc = new StreamingContext(sparkConf, Durations.seconds(slices))


    val topic = Set("youyu_mobile_data_after_etl")

    val kafkaParam = Map("metadata.broker.list" -> kafka_list_addr, "group.id" -> Constants.KAFKA_GROUP_ID)

    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topic)

    val jsonDStream = stream.flatMap(line => {
      val data = JSON.parseObject(line._2)
      Some(data)
    }).filter(jsonObject => {
      //获取当前数据上报的时间，如果上报的时间不在当前天内，则过滤掉
      val reportDate = jsonObject.getString("reportTime").substring(0, 10)
      val todayDate: Date = new Date
      val todayDateStr = DateUtils.getDate(todayDate)
      reportDate == todayDateStr
    })

    //用户字典数据维护
    userInfoPkgDict(jsonDStream, scc)

    //小时，天级别新用户的统计
    newUserCount(jsonDStream, scc)

    //小时，天级别活跃用户的统计
    activeUserCount(jsonDStream, scc)

    //小时，天级别用户启动次数统计
    calculateStarts(jsonDStream, scc)

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
    val active_user_stat_day = ConfigurationManager.getProperty(Constants.HBASE_APP_DATA_DAILY)
    val broadcastActiveUserStat = scc.sparkContext.broadcast(active_user_stat_day)

    val broadcastUserHourMap = mutable.Map[String, mutable.Map[String, Int]]()
    val broadcastUserDayMap = mutable.Map[String, mutable.Map[String, Int]]()

    reports.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        //获取hbase连接connection
        val connection = HbaseUtils.getHbaseConn

        //获取当前时间前一天的时间
        val date: Date = new Date
        val yesterday = DateUtils.getLastDay(date, -1).substring(0, 10)

        partitionOfRecords.foreach(x => {
          val userStatTable = connection.getTable(TableName.valueOf(broadcastActiveUserStat.value))

          val reportDate = x.getString("reportTime").substring(0, 10)
          val hourCode = x.getString("reportTime").substring(11, 13)
          val appKey = x.getString("appKey")
          val pkg = x.getString("pkgId")
          val version = x.getString("appVersion")
          val channel = x.getString("appChannel")
          val clientMd5 = x.getString("clientIdMd5")

          val activeUserType = "active_user"
          val userStatRowKey = appKey + "#" + activeUserType + "#" + reportDate + "#" + pkg + "#" + version + "#" + channel
          val userHourMapKey = appKey + "#" + pkg + "#" + clientMd5 + "#" + hourCode

          try {
            //判断当前时间Key是否在广播变量中
            val hourFlag = broadcastUserHourMap.contains(reportDate)

            //以小时为级别进行对用户的唯一性进行判断来统计活跃用户
            if (!hourFlag) {
              //当前用户作为当前时间段内的活跃用户+1
              HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", hourCode, 1L)

              val valueMap = mutable.Map[String, Int]()
              valueMap += (userHourMapKey -> 1)

              broadcastUserHourMap += (reportDate -> valueMap)

              //如果判断当前跨天了，那就将前天内存中的数据进行清空处理
              for (k <- broadcastUserHourMap.keySet.toArray) {
                if (k.compareToIgnoreCase(yesterday) <= 0) {
                  broadcastUserHourMap.remove(k)
                }
              }
            } else {
              //获取当前时间key对应的value值Map
              val valueMap = broadcastUserHourMap(reportDate)
              val existKey = valueMap.contains(userHourMapKey)
              if (!existKey) {
                //当前用户作为当前时间段内的活跃用户+1
                HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", hourCode, 1L)

                //将该用户【小时】的唯一标识加到内存中
                valueMap += (userHourMapKey -> 1)
              }

            }

            val activeUserUniqType = "active_user_uniq"
            val userDayRowKey = appKey + "#" + activeUserUniqType + "#" + reportDate + "#" + pkg + "#" + version + "#" + channel
            val userDayMapKey = userHourMapKey.substring(0, userHourMapKey.length - 3)

            val dayFlag = broadcastUserDayMap.contains(reportDate)

            //以天为级别进行对用户的唯一性进行判断来统计活跃用户
            if (!dayFlag) {
              //当前用户作为当前时间段内的活跃用户+1
              HbaseUtils.incrementColumnValues(userStatTable, userDayRowKey, "data", hourCode, 1L)
              //天活跃数据+1
              HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", "all", 1L)

              val valueMap = mutable.Map[String, Int]()
              valueMap += (userDayMapKey -> 1)

              broadcastUserDayMap += (reportDate -> valueMap)

              //如果判断当前跨天了，那就将前天内存中的数据进行清空处理
              for (k <- broadcastUserDayMap.keySet.toArray) {
                if (k.compareToIgnoreCase(yesterday) <= 0) {
                  broadcastUserDayMap.remove(k)
                }
              }

            } else {
              //获取当前时间key对应的value值Map
              val valueMap = broadcastUserDayMap(reportDate)
              val existKey = valueMap.contains(userDayMapKey)
              if (!existKey) {
                //当前用户作为当前时间段内的活跃用户+1
                HbaseUtils.incrementColumnValues(userStatTable, userDayRowKey, "data", hourCode, 1L)
                //天活跃数据+1
                HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", "all", 1L)

                //将该用户【天】的唯一标识加到内存中
                valueMap += (userDayMapKey -> 1)
              }
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }
          finally {
            userStatTable.close()
          }
        })
      })
    })
  }

  /**
    * 实时更新用户字典表中的信息
    *
    * @param reports
    * @param scc
    */
  def userInfoPkgDict(reports: DStream[JSONObject], scc: StreamingContext) = {
    val user_pkg_dic = ConfigurationManager.getProperty(Constants.HBASE_APP_PKG_DIC)

    val broadcastUserDict = scc.sparkContext.broadcast(user_pkg_dic)

    //获取字典表中的RowKey
    val userPkgDictRowKeyMap = HbaseUtils.getAllRowKey(user_pkg_dic)

    reports.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        //获取hbase连接connection
        val connection = HbaseUtils.getHbaseConn

        partitionOfRecords.foreach(x => {
          val userDictTable = connection.getTable(TableName.valueOf(broadcastUserDict.value))

          val pkgId = x.getString("pkgId")
          val appName = x.getString("appName")
          val platform = x.getString("deviceType")
          val appKey = x.getString("appKey")

          //如果clientId为null值，设置默认值
          val userDictRowKey = if (pkgId.length == 0) "null" else pkgId

          try {
            //判断当前rowKey在内存中是否存在
            val flag = userPkgDictRowKeyMap.contains(userDictRowKey)

            if (!flag) {
              //判断当前rowKey在表中是否已经存在
              val isNotExistKey = HbaseUtils.isNotExistKey(userDictTable, userDictRowKey)

              if (isNotExistKey) {
                val columns = Array("app_name", "app_platform", "product_key")
                val values = Array(appName, platform, appKey)
                HbaseUtils.addRow(userDictTable, userDictRowKey, "info", columns, values)

                //更新内存中不存在的该rowKey
                userPkgDictRowKeyMap += (userDictRowKey -> 1)
              } else {
                //更新内存中不存在的该rowKey
                userPkgDictRowKeyMap += (userDictRowKey -> 1)
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
    * @param scc
    */
  def newUserCount(reports: DStream[JSONObject], scc: StreamingContext) = {
    val new_user_info_day = ConfigurationManager.getProperty(Constants.HBASE_APP_NEW_USER)
    val new_user_stat_day = ConfigurationManager.getProperty(Constants.HBASE_APP_DATA_DAILY)

    val broadcastUserInfo = scc.sparkContext.broadcast(new_user_info_day)
    val broadcastUserStat = scc.sparkContext.broadcast(new_user_stat_day)

    //获取用户信息表中的RowKey
    val userInfoRowKeyMap = HbaseUtils.getAllRowKey(new_user_info_day)

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
            val flag = userInfoRowKeyMap.contains(userInfoRowKey)

            if (!flag) {
              //如果当前的rowKey在内存中不存在，则在操作表之前判断当前rowkey是否存在当前表中，如果不存在，则执行下面操作
              val isNotExistKey = HbaseUtils.isNotExistKey(userInfoTable, userInfoRowKey)

              if (isNotExistKey) {
                //将信息插入到用户信息表中
                val columns = Array("client_id", "reg_date", "app_version", "app_channel")
                val values = Array(clientId, reportTime, version, channel)
                HbaseUtils.addRow(userInfoTable, userInfoRowKey, "info", columns, values)

                //对统计新用户表中新用户的数量增加1
                HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", "all", 1L)
                HbaseUtils.incrementColumnValues(userStatTable, userStatRowKey, "data", hourCode, 1L)

                //更新内存中不存在的该rowKey
                userInfoRowKeyMap += (userInfoRowKey -> 1)
              } else {
                //更新内存中不存在的该rowKey
                userInfoRowKeyMap += (userInfoRowKey -> 1)
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

  /**
    * 实时统计小时、天级别用户启动次数
    *
    * @param reports
    * @param scc
    */

  def calculateStarts(reports: DStream[JSONObject], scc: StreamingContext) = {
    val appDataDaily = ConfigurationManager.getProperty(Constants.HBASE_APP_DATA_DAILY)
    val dailyDataTable = scc.sparkContext.broadcast(appDataDaily)

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
