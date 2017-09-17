package com.youyu.hive

import java.text.SimpleDateFormat
import java.util.Date

import com.youyu.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by root on 2017/3/29.
  */
object SparkHive {
  val dateUtils = new DateUtils()

  def main(args: Array[String]) {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = new Date

    val beforeOneDate = dateUtils.getLastDay(date, -6) //获取当前时间前一天
    val beforeTwoDate = dateUtils.getLastDay(date, -7) //获取当前时间前2天

    val beforeOneYear = dateUtils.splitDate(beforeOneDate)("year") //获取当前时间前一天所属年的Code
    val beforeTwoYear = dateUtils.splitDate(beforeTwoDate)("year") //获取当前时间前两天所属年的Code

    val beforeOneMonth = dateUtils.splitDate(beforeOneDate)("month") //获取当前时间前一天所属月的Code
    val beforeTwoMonth = dateUtils.splitDate(beforeTwoDate)("month") //获取当前时间前两天所属月的Code

    val beforeOneDay = dateUtils.splitDate(beforeOneDate)("day") //获取当前时间的前一天的Code
    val beforeTwoDay = dateUtils.splitDate(beforeTwoDate)("day") //获取当前时间的前两天的Code

    val conf: SparkConf = new SparkConf().setAppName("SparkHive").setMaster("local[2]")

    val jsc: JavaSparkContext = new JavaSparkContext(conf)

    val hiveContext = new HiveContext(jsc.sc)

    val userDF = hiveContext.sql("select id,app_channel,app_gps,app_ip,app_key,app_name,app_network," +
      "app_source,app_version,city,city_code,ctime,device_id,device_model,device_os,device_res,device_type," +
      "user_id,user_name " + "from app_profile")

    userDF.registerTempTable("app_all_user")

    val appAllUserDF = hiveContext.sql("select id,app_channel,app_gps,app_ip,app_key,app_name,app_network," +
      "app_source,app_version,city,city_code,ctime,device_id,device_model,device_os,device_res,device_type," +
      "user_id,user_name " +
      "from app_profile" +
      "where year(ctime) = " + beforeOneYear +
      " and lpad(month(ctime),2,0) = " + beforeOneMonth +
      " and lpad(day(ctime),2,0) = " + beforeOneDay)

    appAllUserDF.registerTempTable("app_all_user")

    val newUserDF = hiveContext.sql("select pp.ctime,'" + sdf.format(new Date) + "' enter_time, " +
      "pp.device_id,pp.channel app_channel,pp.gps app_gps," +
      "pp.ip app_ip,pp.key app_key,pp.app_name app_name,pp.network app_network,pp.source app_source," +
      "pp.version app_version,pp.city city, pp.city_code city_code,pp.brand device_brand,pp.model " +
      "device_model," + "pp.os device_os,pp.res device_res,pp.type device_type,pp.province province, " +
      "pp.user_id user_id,pp.user_name user_name " + "from ( " + "select " + "app.device_id device_id, " +
      "app.city city, " +
      "app.city_code city_code, " +
      "app.province province, " +
      "app.device_type type, " +
      "app.device_os os, " +
      "app.device_model model, " +
      "app.device_brand brand, " +
      "app.device_res res, " +
      "app.app_version version, " +
      "app.app_name app_name, " +
      "app.app_source source, " +
      "app.app_channel channel, " +
      "app.app_network network, " +
      "app.app_gps gps, " +
      "app.user_id user_id, " +
      "app.user_name user_name, " +
      "app.app_ip ip, " +
      "app.app_key key, " +
      "app.ctime " +
      "from " +
      "(select " +
      "device_id, device_type, device_os, device_model, device_brand, device_res, app_version, " +
      "city,city_code,province, " +
      "app_name, app_source, app_channel, app_network, app_gps, user_id, user_name, app_ip, app_key," +
      "ctime, " + "row_number() over(partition by device_id order by ctime) rank " +
      "from app_profile " +
      ") app where rank = 1 ) pp " +
      "left join app_all_user uu on uu.device_id = pp.device_id " +
      "where uu.device_id is null ")

    newUserDF.show()

    jsc.close
  }
}