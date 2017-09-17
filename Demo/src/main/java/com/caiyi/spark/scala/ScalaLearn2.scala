package com.caiyi.spark.scala

/**
 * Created by root on 2017/3/31.
 */
object ScalaLearn2 {
  def main(args: Array[String]) {
    charAt(2)

    compareTo("Apple")
    compareTo("Orange")

    compareToIgnoreCase("Banana")
    compareToIgnoreCase("banana")
    compareToIgnoreCase("bananb")

    endWith("Pineapple")

    equals("abcd")


  }

  /**
   * 返回指定位置的字符
   * @param index
   */
  def charAt(index: Int): Unit = {
    val str = "abcdef"
    val str2 = str.charAt(2)
    println(str2)
  }

  /**
   * 比较字符串与对象
   * @param str2
   */
  def compareTo(str2: String): Unit = {
    val str = "Apple"

    val flag = str.compareTo(str2)

    println(flag)
  }

  /**
   * 按照字典顺序比较两个字符串的大小，不考虑大小写
   * @param str
   */
  def compareToIgnoreCase(str: String): Unit = {
    val str2 = "banana"

    val flag = str2.compareToIgnoreCase(str)

    println(flag)
  }

  /**
   * 判定字符串是否以指定的后缀介绍
   * @param buffix
   */
  def endWith(buffix: String): Unit = {
    val str = "AppleBananaOrangePineapple"

    val flag = str.endsWith(buffix)

    println(flag)
  }

  /**
   * 将此字符串与指定的对象比较
   * @param str
   */
  def equals(str: String): Unit = {
    val str2 = "abcd"

    val flag = str2.equals(str)

    println(flag)
  }

}
