package com.youyu.mysql

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext

/**
  * spark-submit  --master yarn --deploy-mode cluster --name "ReadMyOracle" --num-executors 2 --executor-memory 2G
  * --executor-cores 2 --class com.youyu.mysql.SparkReadOracle SparkJob-1.0-SNAPSHOT.jar
  *
  * Created by root on 2017/9/15.
  */
object SparkReadOracle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() /*.setMaster("local[2]").setAppName("SparkReadOracle")*/

    conf.set("SPARK_CLASSPATH", "/home/gaoht/ojdbc14.jar")
    /*conf.set("SPARK_CLASSPATH", "D:\\Software\\ojdbc14.jar")*/

    val jsc = new JavaSparkContext(conf)

    val sqlContext = new SQLContext(jsc)

    val oracleURL = "jdbc:oracle:thin:@192.168.1.193:1521:lottery"
    val userName = "credit"
    val password = "credit"
    val tableName = "bk_acctbook_data"

    val jdbcMap = Map("url" -> oracleURL, "user" -> userName, "password" -> password, "dbtable" -> tableName,
      "driver" -> "oracle.jdbc.driver.OracleDriver")


    val sqlDF = sqlContext.read.format("jdbc").options(jdbcMap).load()

    println("-----------------------")
    sqlDF.show()
    println("-----------------------")

    jsc.close()
  }
}
