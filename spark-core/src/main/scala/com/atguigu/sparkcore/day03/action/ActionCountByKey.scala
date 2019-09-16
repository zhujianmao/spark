package com.atguigu.sparkcore.day03.action

import org.apache.spark.SparkContext

object ActionCountByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")

    val rdd1 = sc.makeRDD(Array(("a", 10), ("a", 20), ("b", 100), ("c", 200)))

    val rdd2 = rdd1.countByKey()

    println(rdd2)

    sc.stop
  }

}
