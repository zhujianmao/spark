package com.atguigu.spark.streaming.day01.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformWithStat {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformWithStat")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setCheckpointDir("./chk2")
    val sockectStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    sockectStream.flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey[Int]((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
      .print(100)

    ssc.start()
    ssc.awaitTermination()
  }

}
