package com.atguigu.sparkcore.day04.jobSce

import org.apache.spark.SparkContext

object SechdulerDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")
    val rdd = sc.parallelize(Array("hello atguigu","hello hadoop"))

    val rdd1 = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    rdd1.collect.foreach(println)
    rdd1.collect.foreach(println)

    Thread.sleep(10000000)
    sc.stop
  }
}
