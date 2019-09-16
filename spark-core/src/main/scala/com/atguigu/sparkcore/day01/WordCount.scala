package com.atguigu.sparkcore.day01

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    val rdd = sc.textFile(args(0)).flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_)
    rdd.collect.foreach(println)
    sc.stop
  }
}
