package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext

object SortByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")
    val rdd = sc.parallelize(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))

    //val rdd1 = rdd.sortByKey()
    val rdd1 = rdd.sortByKey(false)

    rdd1.collect.foreach(println)

    sc.stop
  }
}
