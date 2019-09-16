package com.atguigu.sparkcore.day02.baserdd

import org.apache.spark.SparkContext

object RDDCreate {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//                            .setAppName("RDDCreate")
//                            .setMaster("local[2]")
//
//    val sc = new SparkContext(conf)
    val sc =new SparkContext("local[2]","RDDCreate")
    //创建方式一
    val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6))
    //创建方式二
    val rdd2 = sc.parallelize(Array("a","b","c","d","e","f"))

    rdd1.collect().foreach(println)
    println("-----------------------------------")
    rdd2.collect().foreach(println)

    sc.stop
  }
}
