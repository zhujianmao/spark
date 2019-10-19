package com.atguigu.spark.streaming.day02.windows

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Windows4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Windows3").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))

    val sockctDstream = ssc.socketTextStream("hadoop102", 9999).window(Seconds(12), Seconds(4))

    sockctDstream.flatMap(_.split(" "))
      .map((_, 1))
     // .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
      .print

    ssc.start()
    ssc.awaitTermination()
  }
}
