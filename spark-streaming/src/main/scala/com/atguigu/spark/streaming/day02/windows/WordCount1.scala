package com.atguigu.spark.streaming.day02.windows

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Windows3").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))

      val sockctDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    sockctDstream.flatMap(_.split(" "))
          .map((_,1))
          .reduceByKeyAndWindow(_+_,Seconds(6))
//          .reduceByKeyAndWindow((_:Int)+(_:Int),Seconds(12),Seconds(8))
          .print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
