package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext

/**
  * 创建一个 pairRDD，根据 key 计算每种 key 的value的平均值。
  */
object CombineByKey1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "CombineByKey1")

    val rdd = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)

    val valueCount = rdd.combineByKey(
      (_, 1),
      (valueCount: (Int, Int), v: Int) => (valueCount._1 + v, valueCount._2 + 1),
      (valueCount1: (Int, Int), valueCount2: (Int, Int)) => (valueCount1._1 + valueCount2._1, valueCount1._2 + valueCount2._2)
    )


    valueCount.foreach(println)

    val rdd2 = valueCount.map{
      case (key,(k,value)) => (key,k.toDouble / value)
    }

    rdd2.collect.foreach(println)

    sc.stop
  }
}
