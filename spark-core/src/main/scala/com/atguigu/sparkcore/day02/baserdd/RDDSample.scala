package com.atguigu.sparkcore.day02.baserdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDMap")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f"))

    val rdd1 = rdd.sample(false,0.5)
    val rdd2 = rdd.sample(true,0.5)

    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))

    sc.stop
  }
}
