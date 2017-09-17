package com.caiyi.spark.scala

/**
 * Created by root on 2017/3/30.
 */
object RegexTest2 {
  def main(args: Array[String]) {
    val pattern = "(S|s)cala".r
    val str = "Scala is scalable and cool"

    println(pattern replaceFirstIn(str, "Java"))
  }
}
