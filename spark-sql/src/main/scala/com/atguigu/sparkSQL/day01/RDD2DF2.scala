package com.atguigu.sparkSQL.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *   RDD转化为DataFrame  隐式转化
  */
object RDD2DF2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("RDD2DF2").getOrCreate()

    import spark.implicits._


    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(Array("zhangsan" -> 19, "lisi" -> 18))
    //方式一
   // val df: DataFrame = rdd.toDF("name","age")

    //方式二
    val people: RDD[People] = rdd.map{
      case (name,age) => People(name,age)
    }
    val df: DataFrame = people.toDF
    df.show
    spark.close
  }
}


case class People(name: String,age:Int)