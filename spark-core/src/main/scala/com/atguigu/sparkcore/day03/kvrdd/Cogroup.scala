package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext

object Cogroup {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")

    val rdd1 = sc.parallelize(Array((1, 10),(2, 20),(1, 100),(3, 30)),1)
    val rdd2 = sc.parallelize(Array((1, "a"),(2, "b"),(1, "aa"),(3, "c")),1)

    val rdd3 = rdd1.cogroup(rdd2)

    rdd3.collect.foreach(println)
    sc.stop
  }

}
