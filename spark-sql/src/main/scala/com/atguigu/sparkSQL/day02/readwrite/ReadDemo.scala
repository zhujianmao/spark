package com.atguigu.sparkSQL.day02.readwrite

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("WriteDemo").getOrCreate()

    val df: DataFrame = spark.read.format("csv").load("E:\\input\\csv")
    df.show()

    val df1: DataFrame = spark.read.csv("E:\\input\\csv")
    df1.show

    spark.close()
  }
}
