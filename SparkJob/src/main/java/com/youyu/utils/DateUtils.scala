package com.youyu.utils

import java.text.{DecimalFormat, Format, SimpleDateFormat}
import java.util.{Calendar, Date}

/**
 * Created by li on 2017/1/22.
 */
class DateUtils {
  val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")


  /**
   * @param date
   * @return 返回<yyyy-MM-dd HH:mm:ss> 格式的时间
   */
  def getToday(date: Date): String = {
    val result = TIME_FORMAT.format(date)
    result
  }

  /**
   * 解析时间字符串
   *
   * @param time 时间字符串 <yyyy-MM-dd HH:mm:ss>
   * @return Date
   */
  def parseTime(time: String): Date = {
    TIME_FORMAT.parse(time)
  }

  /**
   * 返回当前时间的前几个小时
   *
   * @param date 当前时间
   * @param num  前几小时
   * @return
   */
  def getLastHour(date: Date, num: Int): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.HOUR, num)
    val result = TIME_FORMAT.format(cal.getTime)
    result
  }

  /**
   * 返回当前时间的前几天
   *
   * @param date 当前时间
   * @param num  前几天
   * @return
   */
  def getLastDay(date: Date, num: Int): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, num)
    val result = TIME_FORMAT.format(cal.getTime)
    result
  }

  /**
   * 返回一周前日期
   *
   * @param date
   * @param num
   * @return
   */
  def getLastWeek(date: Date, num: Int): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.WEEK_OF_YEAR, num)
    val result = TIME_FORMAT.format(cal.getTime)
    result
  }

  /**
   * 返回一个月前日期
   *
   * @param date
   * @param num
   * @return
   */
  def getLastMonth(date: Date, num: Int): String = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.MONTH, num)
    val result = TIME_FORMAT.format(cal.getTime)
    result
  }


  /**
   * 分割年月日
   *
   * @param dateStr
   * @return Map ->时，日，周，月，年
   */
  def splitDate(dateStr: String): Map[String, Any] = {
    val year = dateStr.substring(0, 4)
    val month = dateStr.substring(5, 7)
    val day = dateStr.substring(8, 10)
    val hour = dateStr.substring(11, 13)

    val date = TIME_FORMAT.parse(dateStr)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    cal.setMinimalDaysInFirstWeek(4)
    val week = cal.get(Calendar.WEEK_OF_YEAR)
    val f: Format = new DecimalFormat("00")

    val result = Map("year" -> year, "month" -> month, "day" -> day, "hour" -> hour, "week" -> f.format(week))
    result
  }

  /**
   * 比较两个时间相差的毫秒数
   *
   * @param enter
   * @param exit
   * @return
   */
  def dateDiff(enter: String, exit: String): Long = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val enterTime = df.parse(enter)
    val exitTime = df.parse(exit)

    exitTime.getTime - enterTime.getTime
  }

  /*def cassandraBK(appDayProfileBankDF: DataFrame): Unit = {
    val conf = new SparkConf().set("spark.cassandra.connection.host", "192.168.1.88")

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    appDayProfileBankDF.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("keyspace" -> "nirvana", "table" -> "app_profile_bk")).mode(SaveMode.Append).save()
  }*/

}
