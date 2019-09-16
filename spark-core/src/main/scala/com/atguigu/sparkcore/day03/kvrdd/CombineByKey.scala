package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "CombineByKey1")

    val rdd = sc.parallelize(Array("a"->5,"b"->2,"a"->2,"c"->2,"c"->2))

    val rdd2 = rdd.combineByKey(
      v=>(v.min(Int.MaxValue),v.max(Int.MinValue)),

         (minMax:(Int,Int),v:Int) => (minMax._1.min(v),minMax._2.max(v))
      ,
      (minMax1:(Int,Int),minMax2:(Int,Int)) => (minMax1._1 + minMax2._1,minMax1._2 + minMax2._2)
    )

    rdd2.collect.foreach(println)

    sc.stop
  }
}
