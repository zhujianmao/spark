package com.atguigu.sparkcore.day03.action

import org.apache.spark.SparkContext

object ActionTakeOrdered {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")
    val rdd1 = sc.makeRDD(Array(100, 20, 130, 500, 60))

    val rdd2 = rdd1.takeOrdered(3)(Ordering.Int.reverse)
    println(rdd2.mkString(","))

    sc.stop
  }
}
