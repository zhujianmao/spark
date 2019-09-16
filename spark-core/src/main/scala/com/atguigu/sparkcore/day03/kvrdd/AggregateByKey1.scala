package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext

object AggregateByKey1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]","AggregateByKey1")
    val rdd = sc.parallelize(Array("a"->1,"a"->2,"c"->2,"c"->2))
    val rdd1 = rdd.aggregateByKey((Int.MaxValue,Int.MinValue))(
      { case ((min, max), v) => (min.min(v), max.max(v)) },
      (minMax1, minMax2) => (minMax1._1 + minMax2._1, minMax1._2 + minMax2._2)
    )
    rdd1.collect.foreach(println)
  }
}
