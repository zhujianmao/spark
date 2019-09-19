package com.atguigu.sparkcore.day04.transferfunction

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ExtendsSerializable2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "ExtendsSerializable2")

    val rdd = sc.parallelize(Array("hello hadoop", "atguigu","hello atguigu"),3)

    val sd = new SerializebleDemo2("hello")

    val result = sd.getMatchStr2(rdd)

    result.collect.foreach(println)

    sc.stop
  }
}

class SerializebleDemo2(matchStr: String) {
  //在executor上执行
  def isMatch(str: String) = {
    println("isMatch==========================")
    str.contains(matchStr)
  }
  //在driver上调用
  def getMatchStr(rdd: RDD[String]) = {
    println("getMatchStr=============================")
    rdd.filter(isMatch)
  }

  //在driver上调用
  def getMatchStr2(rdd: RDD[String]) = {
    println("getMatchStr2=============================")
    val maStr = matchStr
    rdd.filter(_.contains(maStr))
  }

}
