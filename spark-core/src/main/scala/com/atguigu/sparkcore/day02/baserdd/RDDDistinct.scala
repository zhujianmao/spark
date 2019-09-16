package com.atguigu.sparkcore.day02.baserdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDDistinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDMap")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f","d","e","f"))
    println(rdd.getNumPartitions)
    val rdd1 = rdd.distinct(3)
    println(rdd1.partitioner)
    println(rdd1.getNumPartitions)
    println(rdd1.collect().mkString(","))

    sc.stop
  }

}
