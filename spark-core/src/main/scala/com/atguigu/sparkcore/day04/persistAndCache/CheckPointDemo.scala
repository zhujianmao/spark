package com.atguigu.sparkcore.day04.persistAndCache

import org.apache.spark.SparkContext

object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "CheckPointDemo")
    sc.setCheckpointDir("./check")
    val rdd = sc.parallelize(Array("hello"))

    val rdd1 = rdd.flatMap(_.split(" ")).map((_,{
      System.currentTimeMillis()
    })) //reduceByKey默认有缓存
    val rdd2 = rdd1.map(x=>x)
    rdd1.cache()
    rdd1.checkpoint()
    rdd2.collect.foreach(println)
    println("=====================================")
    rdd2.collect.foreach(println)

    Thread.sleep(10000000)
    sc.stop
  }
}
