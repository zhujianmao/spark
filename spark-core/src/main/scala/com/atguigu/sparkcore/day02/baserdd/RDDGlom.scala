package com.atguigu.sparkcore.day02.baserdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDGlom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDGlom")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f"),4)

    val rdd1 = rdd.glom()

    println(rdd1.count)
    rdd1.collect().foreach(x=> println(x.toList))

    sc.stop
  }
}
