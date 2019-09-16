package com.atguigu.sparkcore.day02.baserdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDGroupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDGroupBy")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 5,
      "a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 5, "a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 5))

    val rdd1 = rdd.groupBy{case (k, _) => k}
    println(rdd1.partitioner)
    println(rdd1.collect().mkString(","))

    sc.stop
  }
}
