package com.youyu.test

import scala.collection.mutable

/**
  * Created by root on 2017/5/19.
  */
object tests2 {
  def main(args: Array[String]): Unit = {
    val activeUserMap = mutable.Map[String, Int]()
    activeUserMap += ("key11" -> 1, "key22" -> 2)

    val itt: Iterator[String] = activeUserMap.keys.iterator
    while (itt.hasNext) {
      println(itt.next().substring(0, itt.next().length - 2))
    }
  }
}