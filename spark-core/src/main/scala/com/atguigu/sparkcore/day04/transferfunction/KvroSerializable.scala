package com.atguigu.sparkcore.day04.transferfunction

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object KvroSerializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[SerializebleDemo1]))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext("local[2]", "KvroSerializable",conf)

    val rdd = sc.parallelize(Array("hello hadoop", "atguigu","hello atguigu"),3)

    val sd = new SerializebleDemo1("hello")

    val result = sd.getMatchStr(rdd)

    result.collect.foreach(println)

    sc.stop
  }
}

class SerializebleDemo1(matchStr: String) extends Serializable {
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
