package com.atguigu.spark.streaming.day01.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformWithoutStat {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TransformWithoutStat").setMaster("local[2]")
    val sctx: StreamingContext = new StreamingContext(conf, Seconds(3))
    val dstream: ReceiverInputDStream[String] = sctx.socketTextStream("hadoop102", 9999)

    dstream.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    }).print

    sctx.start
    sctx.awaitTermination()

  }
}
