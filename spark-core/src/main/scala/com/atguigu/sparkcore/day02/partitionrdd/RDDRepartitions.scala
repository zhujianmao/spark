package com.atguigu.sparkcore.day02.partitionrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDRepartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDMap")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f"),3)

    println(rdd.getNumPartitions)
    val rdd1 = rdd.repartition(2)
    println(rdd1.getNumPartitions)

    val rdd2 = rdd.repartition(4)
    println(rdd2.getNumPartitions)

    println(rdd1.collect().mkString(","))

    sc.stop
  }
}
