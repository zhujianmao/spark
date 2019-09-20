package com.atguigu.sparkSQL.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DF2RDD {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("RDD2DF2").getOrCreate()
    import spark.implicits._

    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(Array("zhangsan" -> 19, "lisi" -> 18))
    val people: RDD[People] = rdd.map{
      case (name,age) => People(name,age)
    }
    val df: DataFrame = people.toDF
    df.show
    val rdd2: RDD[Row] = df.rdd
    rdd2.map(row => {
      println(row.getString(0))
      println(row.getInt(1))
    }).collect()
    spark.close
  }

}
