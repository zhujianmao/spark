package com.atguigu.sparkcore.day04.hbase

import org.apache.spark.SparkContext

object Write2Hbase {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Write2Hbase")


    sc.stop
  }
}
