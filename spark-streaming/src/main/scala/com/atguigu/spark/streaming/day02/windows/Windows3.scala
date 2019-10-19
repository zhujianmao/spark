package com.atguigu.spark.streaming.day02.windows

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Windows3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Windows3").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))
    ssc.checkpoint("./chk3")
    val sockctDstream = ssc.socketTextStream("hadoop102", 9999)

    sockctDstream.flatMap(_.split(" "))
      .map((_, 1))
      //.reduceByKeyAndWindow(_ + _, _ - _, Seconds(12), Seconds(4))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(12), Seconds(4),filterFunc = _._2 > 0)  //加个过滤条件会去除value为0的情况
      .print

    ssc.start()
    ssc.awaitTermination()
  }
}
