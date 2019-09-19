package com.atguigu.sparkcore.day05.acc

import org.apache.spark.SparkContext

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "MyAccumulatorDemo")

    val rdd = sc.parallelize(Array(1,2,3,4))
    val acc = sc.longAccumulator
    val rdd1 = rdd.map(x => {
      acc.add(1)
      println(acc.value)
      (x, 1)
    })
    rdd1.collect.foreach(println)
    rdd1.collect.foreach(println)
    println("=====================================")
    println(acc.value)
    println("--------------------------------")
    sc.stop
  }
}
