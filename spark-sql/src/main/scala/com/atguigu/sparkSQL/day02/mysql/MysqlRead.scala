package com.atguigu.sparkSQL.day02.mysql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object MysqlRead {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("WriteDemo").getOrCreate()

    val df: DataFrame = spark.read.format("jdbc")
                                .option("url","jdbc:mysql://hadoop104:3306/mytest").option("user","root").option("password",123456).option("dbtable","user")
                                .load()
    df.show

    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val df1: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop104:3306/mytest","user",props)
    df1.show
    spark.close()
  }
}
