package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext

object Join {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Join")

    val rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c")))
    val rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))

    println("========== join ==============")
    val rdd3 = rdd1.join(rdd2)
    rdd3.collect.foreach(println)

    println("========== leftOuterJoin ==============")
    val rdd4 = rdd1.leftOuterJoin(rdd2)
    rdd4.collect.foreach(println)

    println("========== rightOuterJoin ==============")
    val rdd5 = rdd1.rightOuterJoin(rdd2)
    rdd5.collect.foreach(println)

    println("========== fullOuterJoin ==============")
    val rdd6 = rdd1.fullOuterJoin(rdd2)
    rdd6.collect.foreach(println)

    sc.stop
  }
}
