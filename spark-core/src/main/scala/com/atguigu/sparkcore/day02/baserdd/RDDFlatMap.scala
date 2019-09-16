package com.atguigu.sparkcore.day02.baserdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDFlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDFlatMap")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a a c","b b ","c c","d d","e","f"))

    val rdd1 = rdd.flatMap(_.split(" "))

    println(rdd1.collect().mkString(","))

    sc.stop
  }
}
