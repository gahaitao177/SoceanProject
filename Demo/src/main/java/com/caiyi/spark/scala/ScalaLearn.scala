package com.caiyi.spark.scala

import scala.collection.immutable.HashMap

/**
 * Created by root on 2017/3/30.
 */
object ScalaLearn {
  def main(args: Array[String]) {
    hello()
    upperCase()
    println(max(3, 4))
    println(max2(3, 4))
    println(max3(3, 4))
    array()
    list()
    set()
    set2()
    map()
    map2()
    printArgs()
    printArgs2()
    printArgs3()
    iteratorTest()
    StringBuilderTest()
    zhifuchuanLink()
  }

  def hello(): Unit = {
    println("Hello World !")
  }

  /**
   * 判断是否有大小写
   */
  def upperCase(): Unit = {
    val name = "Zhangsan"

    val nameHashUpper = name.exists(_.isUpper)

    println(nameHashUpper)
  }

  /**
   * map
   */
  def mapDefine(): Unit = {
    val x = new HashMap[Int, String]()

    val x2: Map[Int, String] = new HashMap()
  }

  /**
   * 定义函数比较大小值
   * @param x
   * @param y
   * @return
   */
  def max(x: Int, y: Int): Int = {
    if (x > y) x
    else y
  }

  /**
   * 如果编译器能够推断函数的返回来下 可以不用写
   * @param x
   * @param y
   * @return
   */
  def max2(x: Int, y: Int) = {
    if (x > y) x
    else y
  }

  /**
   * 如果一个函数仅有一个句子组成，可以选择不写大括号
   * @param x
   * @param y
   * @return
   */
  def max3(x: Int, y: Int) = if (x > y) x else y

  /**
   * 带类型的参数化数组
   */
  def array(): Unit = {
    val greetStrings = new Array[String](3)
    //如果你想用一种更显示的方式，你可以显示定义greetStrings的类型
    //val greetStrings:Array[String] = new Array[String](3)
    greetStrings(0) = "Hello"
    //greetStrings.update(0, "Hello")
    greetStrings(1) = ". "
    //greetStrings.update(1, ". ")
    greetStrings(2) = "World!"
    //greetStrings.update(2, "World !")

    for (i <- 0 to 2) {
      println(greetStrings(i))
      //println(greetStrings.apply(i))
    }
  }

  /**
   * scala中的scala.List（可变的）和Java中的java.util.List不同（不可变的）
   */
  def list(): Unit = {
    val oneTwo = List(5, 6)
    val threeFour = List(3, 4)

    val oneTwoThreeFour = oneTwo ::: threeFour

    println(oneTwo + " and " + threeFour + " are not mutated .")
    println("Thus. " + oneTwoThreeFour + " is a new List.")

    val twoThree = List(2, 3)
    val oneTwoThree = 1 :: twoThree
    println(oneTwoThree)

    val threeTwoOne = twoThree.::(1)

    println(threeTwoOne)

    val oneTwoThree2 = 4 :: 1 :: 2 :: 3 :: Nil
    println(oneTwoThree2)

    val thrill = "Will" :: "fill" :: "until" :: "to" :: Nil //List("Will","fill","until","to")
    //计算长度为4的string元素的个数
    val count = thrill.count(s => s.length == 4)
    println(count)

    //返回去掉前2个元素的thrill列表
    val leftList = thrill.drop(2)
    println(leftList)

    //返回去掉右边2个元素的thrill列表
    val rightList = thrill.dropRight(2)
    println(rightList)

    //判断是否值为"fill"的元素在thrill列表中
    val flag = thrill.exists(s => s == "fill")
    println(flag)

    //判断所以长度为4的元素组成的列表
    val fourSizeList = thrill.filter(s => s.length == 4)
    println(fourSizeList)

    //判断是否thrill列表中的所有元素都是以“l”结尾
    val newFlag = thrill.forall(s => s.endsWith("l"))
    println(newFlag)

    //返回的结果是：Willfilluntilto
    //thrill.foreach(s => print(s)) //等同于thrill.foreach(print)

    //返回列表中第一个元素
    val headList = thrill.head
    println(headList)

    //返回除掉第一个元素thrill列表
    val deleteList = thrill.tail
    println(deleteList)

    //将thrill里面的元素后面加上“y返回”
    val mapList = thrill.map(s => s + "y")
    println(mapList)

    //将thrill中的元素进行按照“，”进行拼接成字符串输出
    val makeString: String = thrill.mkString(",")
    println(makeString)

    //将thrill元素倒置输出
    val revList = thrill.reverse
    println(revList)

  }

  def set(): Unit = {
    import scala.collection.immutable.Set
    var jetSet = Set("Beijing", "Shanghai")
    jetSet += "Guangzhou"
    println(jetSet.contains("Nanjing") + "   " + jetSet.contains("Guangzhou") + "  " + jetSet)
  }

  def set2(): Unit = {
    import scala.collection.mutable.Set
    val movieSet = Set("Hitch", "Poltergeist")
    movieSet += "Shrek"
    println(movieSet)

    import scala.collection.immutable.HashSet
    val hashSet = HashSet("Tomatoes", "Chilies")
    println(hashSet + "Coriander")

  }

  def map(): Unit = {
    val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", "peru" -> "#CD853F")

    val nums: Map[Int, Int] = Map()

    println("colors中的键为：" + colors.keys)
    println("colors中的值为：" + colors.values)
    println("检测colors中的键为：" + colors.isEmpty)
    println("检测nums中的键为：" + nums.isEmpty)
  }

  /**
   * map合并
   */
  def map2(): Unit = {
    val colors1 = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", "peru" -> "#CD853F")

    val colors2 = Map("blue" -> "#0033FF", "yellow" -> "#FFFF00", "red" -> "#FF0000")

    //++作为运算符
    var col1 = colors1 ++ colors2
    println("colors1 + colors2:" + col1)

    //++作为方法
    var col2 = colors1.++(colors2)
    println("colors1.++(colors2) :" + col2)
  }

  /**
   * 指令式风格
   */
  def printArgs(): Unit = {
    val args = Array("hello", "world", "alone")
    var i = 0
    while (i < args.length) {
      println(args(i) + "\t")
      i += 1
    }
  }

  /**
   * 函数式风格
   */
  def printArgs2(): Unit = {
    println("********")
    val args = Array("Hello", "World", "Alone")
    var i = 0
    for (arg <- args) {
      println(arg + "\t")
    }
  }

  //此方法同上 -- 函数式风格
  def printArgs3(): Unit = {
    println("********")
    val args = Array("Hello", "World", "Alone")
    args.foreach(println)
  }

  def iteratorTest(): Unit = {
    val arr = List("Baidu", "Google", "Runoob", "Taobao")
    val itt = arr.iterator

    while (itt.hasNext) {
      println("------" + itt.next())
    }

    val arr2 = Array(20, 40, 2, 50, 69, 90)
    val list2 = List(20, 40, 2, 50, 69, 90)

    println("最大的元素是：" + arr2.max + "  " + list2.max)
    println("最小的元素是：" + arr2.min + "  " + list2.min)

    //获取迭代器的长度

    val arr3 = Array(20, 40, 2, 50, 69, 90)
    val arr4 = Array(20, 40, 2, 50, 69, 90)
    val itt3 = arr3.iterator
    val itt4 = arr4.iterator

    println("itt.length的值：" + itt3.length + "  " + arr3.length)
    println("itt.size的值：" + itt4.size + "  " + arr4.size)

    //对迭代器的操作还有
    /*itt3.drop(3)
    itt3.count(s => s == 20)
    itt3.contains(20)
    itt3.find(s => s == 2)
    itt3.filter(s => s == 90)
    itt3.exists(s => s == 40)
    itt3.min
    itt3.max
    .....*/

  }

  def StringBuilderTest(): Unit = {
    val buf = new StringBuilder
    buf += 'a'
    buf ++= "bcdef"
    println("buf is : " + buf.toString)
  }

  def zhifuchuanLink(): Unit = {
    val str1 = "菜鸟教学课程"
    val str2 = "www.runoob.com"

    val newStr1 = str1 + str2
    val newStr2 = str1.concat(str2)

    println("两种方式拼接的字符串为：\n newStr1=" + newStr1 + " \n newStr2=" + newStr2)
  }

}
