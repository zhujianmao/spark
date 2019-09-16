package com.atguigu.sparkcore.day03.action

import org.apache.spark.SparkContext

object ActionReduce {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")
    val rdd1 = sc.parallelize(1 to 10)

    println("========== reduce ==========")
    val result = rdd1.reduce(_+_)
    println(result)
    println("========== count ==========")
    val r1 = rdd1.count()
    println(r1)
    println("========== take ==========")
    val array = rdd1.take(3)
    println(array.mkString(","))
    println("========== first ==========")
    val r2 = rdd1.first()
    println(r2)
    println("========== foreach ==========")
    rdd1.foreach(println)

    sc.stop
  }

}
