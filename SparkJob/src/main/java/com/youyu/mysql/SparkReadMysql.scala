package com.youyu.mysql

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext

/**
  * spark-submit  --master yarn --deploy-mode cluster --name "ReadMyOracle" --num-executors 2 --executor-memory 2G
  * --executor-cores 2 --class com.youyu.mysql.SparkReadMysql SparkJob-1.0-SNAPSHOT.jar
  *
  * 该代码再本地测试不行，得部署到集群中测试
  *
  * Created by root on 2017/9/15.
  */
object SparkReadMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    val jsc = new JavaSparkContext(conf)

    val sqlContext = new SQLContext(jsc)

    val url = "jdbc:mysql://192.168.1.45:3306/azkaban"
    val userName = "scm"
    val password = "scm"
    val tableName = "execution_flows"

    val jdbcMap = Map("url" -> url, "user" -> userName, "password" -> password, "dbtable" -> tableName,
      "driver" -> "com.mysql.jdbc.Driver")

    val sqlDF = sqlContext.read.format("jdbc").options(jdbcMap).load()

    println("-----------------------")
    sqlDF.show()
    println("-----------------------")

    jsc.close()
  }
}
