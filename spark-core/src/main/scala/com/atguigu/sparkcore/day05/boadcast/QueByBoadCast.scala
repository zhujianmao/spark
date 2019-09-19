package com.atguigu.sparkcore.day05.boadcast

import org.apache.spark.SparkContext

object QueByBoadCast {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "QueByBoadCast")
    val rdd = sc.parallelize(Array(1,2,3,4,5))
    val broad = sc.broadcast(Set(1,2,3))
    rdd.foreach(x=>{
      val set = broad.value
      println(set.contains(x))
    })
    sc.stop
  }
}
