package com.atguigu.sparkcore.day03.action

import org.apache.spark.SparkContext

object ActionAggregate {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")

    val rdd1 = sc.makeRDD(Array(100, 30, 10, 30, 1, 50, 1, 60, 1), 2)
    //val result = rdd1.aggregate(0)(_+_,_+_)
    val result = rdd1.aggregate(100)(_+_,_+_)
    println(result)

    sc.stop
  }
}
