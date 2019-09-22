package com.atguigu.sparkSQL.day02.hive

import org.apache.spark.sql.SparkSession

object Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
      .appName("Hive")
      .getOrCreate()

    spark.sql("show databases")
    spark.sql("show tables").show()

    spark.close
  }
}
