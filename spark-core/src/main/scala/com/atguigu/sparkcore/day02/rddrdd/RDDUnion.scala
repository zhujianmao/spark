package com.atguigu.sparkcore.day02.rddrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDUnion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDMap")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(1,2,3,4,5),3)
    val rdd2 = sc.parallelize(Array(4,5,6,7,7),3)
    println(rdd.getNumPartitions)
    val rdd1 = rdd.union(rdd2)
    val rdd3 = rdd ++ rdd2
    println(rdd3.getNumPartitions)
    println(rdd1.mapPartitionsWithIndex((x,y)=>y.map((x,_))).collect().mkString(","))
    println(rdd3.collect().mkString(","))

    sc.stop
  }

}
