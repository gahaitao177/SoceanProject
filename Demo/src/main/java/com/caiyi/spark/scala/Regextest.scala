package com.caiyi.spark.scala

/**
 * Created by root on 2017/3/30.
 */
object Regextest {
  def main(args: Array[String]) {
    val pattern = "Scala".r
    val str = "Scala is Scalable and cool"

    //findFirstInt找到首个匹配项
    println(pattern findFirstIn (str))
  }
}
