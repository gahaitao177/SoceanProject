package com.youyu.test

import scala.collection.mutable

/**
  * Created by root on 2017/5/22.
  */
object test3 {
  def main(args: Array[String]): Unit = {

    val activeUserMap = mutable.Map[String, mutable.Map[String, Int]]()
    activeUserMap += ("2017-05-24" -> mutable.Map("key1" -> 1, "key2" -> 2))

    val reportTime = "2017-05-24"

    val flag = activeUserMap.contains(reportTime)
    import scala.collection.JavaConversions._

    if (!flag) {
      val valueMap = mutable.Map[String, Int]()

      valueMap += ("key1" -> 1)
      valueMap += ("key11" -> 2)

      activeUserMap += (reportTime -> valueMap)

      for (entry <- activeUserMap.entrySet) {
        System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
      }

    } else {
      val valueMap = activeUserMap(reportTime)
      val isNotExist = valueMap.contains("key3")
      if (!isNotExist) {
        valueMap += ("key3" -> 3)
        //activeUserMap += (reportTime -> valueMap)
      }
    }

    for (entry <- activeUserMap.entrySet) {
      System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
    }

    /*println("-------------------------------------1")
    valueMap += ("key1" -> "2017-05-24")

    activeUserMap += ("2017-05-24" -> valueMap)

    import scala.collection.JavaConversions._

    for (entry <- activeUserMap.entrySet) {
      System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
    }

    println("-------------------------------------2")
    valueMap += ("key2" -> "2017-05-24")

    for (entry <- activeUserMap.entrySet) {
      System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
    }

    activeUserMap += ("2017-05-24" -> valueMap)

    println("-------------------------------------3")

    val flag = activeUserMap.contains("2017-05-25")

    if (!flag) {
      val valueMap2 = mutable.Map[String, String]()
      valueMap2 += ("key3" -> "2017-05-25")

      activeUserMap += ("2017-05-25" -> valueMap2)

      for (entry <- activeUserMap.entrySet) {
        System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
      }

      println("-------------------------------------4")
      valueMap2 += ("key1" -> "2017-05-25")

      for (entry <- activeUserMap.entrySet) {
        System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
      }

      println(activeUserMap("2017-05-25"))
    }*/

    /*println(activeUserMap("2017-05-24"))

    if (flag) {
      println(activeUserMap("2017-05-24").contains("a"))
    }

    valueMap += ("b" -> 2)
    println(activeUserMap("2017-05-24"))
    for (entry <- activeUserMap.entrySet) {
      System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
    }

    //移除指定 key
    activeUserMap.remove("2017-05-24")
    for (entry <- activeUserMap.entrySet) {
      System.out.println("Key = " + entry.getKey + ", Value = " + entry.getValue)
    }*/


  }
}
