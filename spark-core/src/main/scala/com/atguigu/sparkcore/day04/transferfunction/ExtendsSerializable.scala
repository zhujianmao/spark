package com.atguigu.sparkcore.day04.transferfunction

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ExtendsSerializable {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "KvroSerializable")

    val rdd = sc.parallelize(Array("hello hadoop", "atguigu","hello atguigu"),3)

    val sd = new SerializebleDemo1("hello")

    val result = sd.getMatchStr(rdd)

    result.collect.foreach(println)

    sc.stop
  }
}

class SerializebleDemo(matchStr: String) extends Serializable {
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

}
