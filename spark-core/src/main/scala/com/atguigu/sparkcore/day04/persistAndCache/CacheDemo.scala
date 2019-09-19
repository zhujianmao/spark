package com.atguigu.sparkcore.day04.persistAndCache

import org.apache.spark.SparkContext

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "PersistDemo")
    val rdd = sc.parallelize(Array("hello"))

    val rdd1 = rdd.flatMap(_.split(" ")).map((_,{
      println("zzzzzzzzzz")
      1
    })) //reduceByKey默认有缓存
    val rdd2 = rdd1.map(x=>x)
    rdd1.cache()
    rdd2.collect.foreach(println)
    println("=====================================")
    rdd2.collect.foreach(println)

    Thread.sleep(10000000)
    sc.stop
  }
}
