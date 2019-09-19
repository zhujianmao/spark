package com.atguigu.sparkcore.day05.acc

import org.apache.spark.SparkContext

object MyAccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "MyAccumulatorDemo")

    val rdd = sc.parallelize(Array(1,2,3,4))
    val myAcc = new MyAccumulator
    sc.register(myAcc)
    val rdd1 = rdd.map(x => {
      myAcc.add(1)
      println(myAcc.value)
      (x, 1)
    })
    rdd1.collect.foreach(println)
    rdd1.collect.foreach(println)
    println("=====================================")
    println(myAcc.value)
    sc.stop
  }
}
