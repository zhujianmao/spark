package com.atguigu.spark.streaming.day01.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountSocket {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountSocket")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val sockectStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)

    val result: DStream[(String, Int)] = sockectStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print(100)

    ssc.start()
    ssc.awaitTermination()

  }
}
