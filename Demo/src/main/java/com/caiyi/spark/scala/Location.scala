package com.caiyi.spark.scala

/**
 * Created by root on 2017/3/30.
 */
class Location(val xc: Int, val yc: Int, val zc: Int) extends Point(xc, yc) {
  var z: Int = zc

  def move(dx: Int, dy: Int, dz: Int): Unit = {
    x = x + dx
    y = y + dy
    z = z + dz

    println("x 的坐标点：" + x)
    println("y 的坐标点：" + y)
    println("z 的坐标点：" + z)
  }

}
