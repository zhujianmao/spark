package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")

    val rdd = sc.parallelize(Array("hello hello atguigu","hadoop hello hive","love hadoop"))

    val rdd1 = rdd.flatMap(_.split(" ")).map((_,1))

//    val rdd2 = rdd1.aggregateByKey(0)(_+_,_+_)
    val rdd2 = rdd1.aggregateByKey("zzz")(_+_,_+_)

    rdd2.collect.foreach(println)

    sc.stop
  }
}
