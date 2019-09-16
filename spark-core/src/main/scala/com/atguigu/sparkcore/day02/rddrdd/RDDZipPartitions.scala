package com.atguigu.sparkcore.day02.rddrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDZipPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDZipPartitions")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a","b","c","d","e","f","a"))
    val rdd2 = sc.parallelize(Array("g","h","i","d","e","f"))
    println(rdd.getNumPartitions)
    val rdd1 = rdd.zipPartitions(rdd2)((it1,it2)=>{
     // it1.zip(it2)
     // it1
      it1.zipAll(it2,-1,-2)
    })
    println(rdd1.getNumPartitions)
    println(rdd1.collect().mkString(","))
    sc.stop
  }

}
class A{
  var a : Int = _
}
