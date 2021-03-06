package com.atguigu.sparkcore.day02.rddrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDCartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDCartesian")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f"),3)
    val rdd2 = sc.parallelize(Array("g","h","i","d","e","f"))
    println(rdd.getNumPartitions)
    val rdd1 = rdd.cartesian(rdd2)
    println(rdd1.getNumPartitions)
    rdd1.collect().foreach({
      case (k,v) => println((k,v))
    })

    sc.stop
  }

}
