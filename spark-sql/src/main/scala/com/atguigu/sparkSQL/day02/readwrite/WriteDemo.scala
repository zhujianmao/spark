package com.atguigu.sparkSQL.day02.readwrite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("RDD2DF2").getOrCreate()
    import spark.implicits._
    val rdd: RDD[User] = spark.sparkContext.parallelize(Array(User("zhangsan",18),User("lisi",20)))

    val df: DataFrame = rdd.toDF

    df.write.mode("overwrite").format("json").save("E:\\output\\json")

    df.write.mode("overwrite").json("E:\\output\\json")

    spark.close
  }
}
case class User(name:String,age:Int)

