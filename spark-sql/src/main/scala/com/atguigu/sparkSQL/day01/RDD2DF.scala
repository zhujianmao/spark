package com.atguigu.sparkSQL.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  *   RDD转化为DataFrame
  */
object RDD2DF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("RDD2DF2").getOrCreate()
    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(Array("zhangsan" -> 19, "lisi" -> 18))
    val rdd2: RDD[Row] = rdd.map {
      case (name, age) => Row(name, age)
    }
    val schema: StructType = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
    val df: DataFrame = spark.createDataFrame(rdd2, schema)

    df.show
    spark.close
  }
}
