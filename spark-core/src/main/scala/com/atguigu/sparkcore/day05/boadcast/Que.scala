package com.atguigu.sparkcore.day05.boadcast

import org.apache.spark.SparkContext

object Que {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")
    val rdd = sc.parallelize(Array(1,2,3,4,5))
    val set = Set(1,2,3)
    rdd.foreach(x=>{
      println(set.contains(x))
    })
    sc.stop
  }
}
