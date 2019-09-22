package com.atguigu.sparkSQL.day02.mysql

import java.util.Properties

import com.atguigu.sparkSQL.day02.readwrite.User
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MysqlWrite {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("MysqlWrite").getOrCreate()
    import spark.implicits._
    val rdd: RDD[User1] = spark.sparkContext.parallelize(Array(User1(18),User1(20)))

    val df: DataFrame = rdd.toDF

    df.write.format("jdbc")
            .mode(SaveMode.Append)
            .option("url","jdbc:mysql://hadoop104:3306/mytest")
            .option("user","root")
            .option("password","123456")
            .option("dbtable","user")
            .save()

    spark.close()
  }
}
case class User1(id:Int)