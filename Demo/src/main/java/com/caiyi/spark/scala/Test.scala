package com.caiyi.spark.scala

/**
 * Created by root on 2017/3/30.
 */
object Test {
  def main(args: Array[String]) {
    val pt = new Point(10, 20)

    //移动到一个新的位置
    pt.move(10, 10)

    val lt = new Location(10, 20, 30)

    lt.move(10, 10, 10)

    //scala闭包
    println("multiplier(3) value = " + multiplier(3))

  }

  var factor = 3

  val multiplier = (i: Int) => i * factor
}
