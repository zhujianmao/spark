package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FoldByKey {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext("local[2]", "FoldByKey")

    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a",3), ("a",2), ("c",4), ("b",3), ("c",6), ("c",8)))

   // val rdd2 = rdd.foldByKey(0)(_+_)
    val rdd2: RDD[(String, Int)] = rdd.foldByKey(10)(_+_)

    rdd2.collect.foreach(println)

    sc.stop
  }
}
