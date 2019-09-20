package com.atguigu.sparkSQL.day01.dedineFunc

import com.atguigu.sparkSQL.day01.People
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

object MySum {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("RDD2DF2").getOrCreate()
    import spark.implicits._

    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(Array("zhangsan" -> 19, "lisi" -> 18))
    val people: RDD[People] = rdd.map{
      case (name,age) => People(name,age)
    }
    val df: DataFrame = people.toDF

    val ds: Dataset[People] = df.as[People]
    ds.createOrReplaceTempView("people")

    spark.udf.register("mysum",new MyAvg())

    spark.sql("select mysum(age) from people").show

    spark.close
  }
}

class MySum extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(List(StructField("column",DoubleType)))

  override def bufferSchema: StructType = StructType(List(StructField("sum",DoubleType)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0D

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =  buffer(0) = buffer.getDouble(0) + input.getDouble(0)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =  buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)

  override def evaluate(buffer: Row): Double = buffer.getDouble(0)
}
