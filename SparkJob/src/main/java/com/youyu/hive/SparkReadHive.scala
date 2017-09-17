package com.youyu.hive

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * 线上测试没问题
  * spark-submit  --master yarn --deploy-mode cluster --name "SparkReadHive" --num-executors 2 --executor-memory 2G
  * --executor-cores 2 --class com.youyu.hive.SparkReadHive  SparkJob-1.0-SNAPSHOT.jar
  *
  * Created by root on 2017/9/14.
  */
object SparkReadHive {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkHive")

    val jsc: JavaSparkContext = new JavaSparkContext(conf)

    val hiveContext = new HiveContext(jsc.sc)

    hiveContext.sql("use tmp")

    val sqlDF = hiveContext.sql("select a_app_key,a_pkg_id,a_app_version from  app_report_record")

    sqlDF.show()

    jsc.close()
  }
}
