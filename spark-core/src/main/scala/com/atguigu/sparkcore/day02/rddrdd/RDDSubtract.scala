package com.atguigu.sparkcore.day02.rddrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDSubtract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDSubtract")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f"))
    val rdd2 = sc.parallelize(Array("g","h","i","d","e","f"))
    println(rdd.getNumPartitions)
    val rdd1 = rdd.subtract(rdd2)
    println(rdd1.partitioner)
    println(rdd1.getNumPartitions)
    println(rdd1.collect().mkString(","))

    sc.stop
  }

}
