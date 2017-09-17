package bill

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by root on 2017/2/6.
 */
object testRow {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AppHourStatisticsAnalyse").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val scoreList = Array(Tuple2("xuruyun", Tuple3(1,11,111)), Tuple2("liangyongqi",Tuple3(2,22,222)),
      Tuple2("wangfei",Tuple3(3,33,333)),Tuple2("wangfei",Tuple3(4,44,444)),Tuple2("wangfei",Tuple3(5,55,555)))

    val scoresRDD = sc.parallelize(scoreList)

    val rowRDD = scoresRDD.map(p => Row(p._1,p._2))
  }
}
