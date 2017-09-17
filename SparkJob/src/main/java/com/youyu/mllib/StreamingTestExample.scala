package com.youyu.mllib

import org.apache.spark.SparkConf
import org.apache.spark.mllib.stat.test.{BinarySample, StreamingTest}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2017/9/8.
  * spark-submit  --master yarn --deploy-mode cluster --name "app_realtime_analyse" --num-executors 2 --executor-memory 2G  --executor-cores 2 --class com.youyu.mllib.StreamingTestExample  SparkJob-1.0-SNAPSHOT.jar  /user/gaoht/streaming_data.txt 10 20
  */
object StreamingTestExample {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      // scalastyle:off println
      System.err.println(
        "Usage: StreamingTestExample " +
          "<dataDir> <batchDuration> <numBatchesTimeout>")
      // scalastyle:on println
      System.exit(1)
    }

    val dataDir = args(0)
    val batchDuration = Seconds(args(1).toLong)
    val numBatchesTimeout = args(2).toInt

    val conf = new SparkConf().setAppName("StreamingTestExample")
    val ssc = new StreamingContext(conf, batchDuration = batchDuration)

    ssc.checkpoint("/user/gaoht/file") //设置在hdfs上的路径

    val data = ssc.textFileStream(dataDir).map(line => line.split(",") match {
      case Array(label, value) => BinarySample(label.toBoolean, value.toDouble)
    })

    val streamingTest = new StreamingTest().setPeacePeriod(0).setWindowSize(0).setTestMethod("welch")

    val out = streamingTest.registerStream(data)
    println("******************************")
    out.print()
    println("******************************")

    var timeoutCounter = numBatchesTimeout
    out.foreachRDD { rdd =>
      timeoutCounter -= 1
      val anySignficant = rdd.map(_.pValue < 0.05).fold(false)(_ || _)
      if (timeoutCounter == 0 || anySignficant) rdd.context.stop()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
