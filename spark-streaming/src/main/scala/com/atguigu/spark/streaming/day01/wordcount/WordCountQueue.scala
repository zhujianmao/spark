package com.atguigu.spark.streaming.day01.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCountQueue {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountQueue").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))
    val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
    //oneAtTime是否一次取队列中的一个时间间隔
    ssc.queueStream(queue,oneAtATime = false).reduce(_+_).print(100)

    ssc.start()
    while (true) {
      queue.enqueue(ssc.sparkContext.parallelize(1 to 100))
      Thread.sleep(1000)
    }
    ssc.awaitTermination()
  }
}
