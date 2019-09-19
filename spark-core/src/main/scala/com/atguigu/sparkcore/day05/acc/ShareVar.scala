package com.atguigu.sparkcore.day05.acc

import org.apache.spark.SparkContext

object ShareVar {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "MyAccumulatorDemo")

    val rdd = sc.parallelize(Array(1,2,3,4))
    var a = 1
    val rdd1 = rdd.map(x => {
      a += 1
      println(a)
      (x, 1)
    })
    rdd1.collect.foreach(println)
    println("=====================================")
    println(a)
    sc.stop
  }
}
