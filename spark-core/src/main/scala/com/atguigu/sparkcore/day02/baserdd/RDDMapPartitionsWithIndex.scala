package com.atguigu.sparkcore.day02.baserdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDMapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDMapPartitionsWithIndex")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f"),3)

    val rdd1 = rdd.mapPartitionsWithIndex((n,it)=>{it.map((n,_))})

    println(rdd1.collect().mkString(","))

    sc.stop
  }
}
