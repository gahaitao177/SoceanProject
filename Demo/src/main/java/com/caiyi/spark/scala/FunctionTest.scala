package com.caiyi.spark.scala

/**
 * Created by root on 2017/3/31.
 */
object FunctionTest {
  def main(args: Array[String]) {
    delayed(time())
  }

  def time() = {
    println("Getting time in nano seconds")
    System.nanoTime()
  }

  def delayed(t: => Long): Unit = {
    println("In delayed method")
    println("Param:" + t)
    t
  }
}
