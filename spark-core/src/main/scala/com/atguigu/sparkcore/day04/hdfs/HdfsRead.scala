package com.atguigu.sparkcore.day04.hdfs

import org.apache.spark.SparkContext

object HdfsRead {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", this.getClass.getSimpleName.split("\\$")(0))

    val rdd = sc.textFile("E:\\input\\combine")
    println(rdd.getNumPartitions)
    sc.stop
  }
}
