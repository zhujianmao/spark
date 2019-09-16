package com.atguigu.sparkcore.day02.partitionrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDCoalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDMap")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f"))
    println(rdd.getNumPartitions)
    val rdd1 = rdd.coalesce(1);
    val rdd2 = rdd.coalesce(3);
    val rdd3 = rdd.coalesce(3,true);
    println(rdd1.getNumPartitions)
    println(rdd2.getNumPartitions)
    println(rdd3.getNumPartitions)
    println(rdd1.collect().mkString(","))

    sc.stop
  }
}
