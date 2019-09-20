package com.atguigu.sparkSQL.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object RDD2DS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                .master("local[*]").appName("RDD2DS")
                                .getOrCreate()
    import spark.implicits._
    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(Array("zhangsan" -> 19, "lisi" -> 18))
    val rdd2: RDD[People] = rdd.map {
      case (name, age) => People(name, age)
    }
    val ds: Dataset[People] = rdd2.toDS()
    ds.show()
    ds.createOrReplaceTempView("people")
    spark.sql("select * from people").show
    spark.close
  }
}
