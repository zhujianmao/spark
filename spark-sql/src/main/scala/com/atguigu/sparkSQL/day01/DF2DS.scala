package com.atguigu.sparkSQL.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DF2DS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("RDD2DF2").getOrCreate()
    import spark.implicits._

    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(Array("zhangsan" -> 19, "lisi" -> 18))
    val people: RDD[People] = rdd.map{
      case (name,age) => People(name,age)
    }
    val df: DataFrame = people.toDF

    val ds: Dataset[People] = df.as[People]
    ds.show()

    spark.close
  }
}
