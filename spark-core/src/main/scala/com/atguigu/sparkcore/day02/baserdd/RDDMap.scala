package com.atguigu.sparkcore.day02.baserdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
                              .setAppName("RDDMap")
                              .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f"))

    val rdd1 = rdd.map(t=>t.toUpperCase)

    println(rdd1.collect().mkString(","))

    sc.stop
  }
}
