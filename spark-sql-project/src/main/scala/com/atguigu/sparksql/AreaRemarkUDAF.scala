package com.atguigu.sparksql

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AreaRemarkUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("cite_name", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("city_count", MapType(StringType, LongType)) :: StructField("all_count", LongType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // if (input.isNullAt(0)) return
    val cityName: String = input.getString(0)
    val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
    //每个商品在各个城市的次数
    buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
    //每个商品在所有城市的总次数
    buffer(1) = buffer.getLong(1) + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //总次数
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)
    val newMap: collection.Map[String, Long] = map2.foldLeft(map1) {
      case (map, (cityName, count)) => map + (cityName -> (map1.getOrElse(cityName, 0L) + count))
    }
    buffer1(0) = newMap
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): String = {
    val cityMap: collection.Map[String, Long] = buffer.getMap[String, Long](0)
    val allCount: Long = buffer.getLong(1)
    val allCityCount: List[(String, Long)] = cityMap.toList.sortBy(-_._2)
    val twoCityCount: List[(String, Long)] = allCityCount.take(2)
    var result: List[AreaRemark] = twoCityCount.map {
      case (city, count) => AreaRemark(city, count.toDouble / allCount)
    }
    if (allCityCount.size>2){
//       result = result :+ AreaRemark("其他",1-result(0).rate-result(1).rate)
       result = result :+ AreaRemark("其他",result.foldLeft(1D)(_-_.rate))
    }
      result.mkString(",")
  }
}

case class AreaRemark(cityName: String, rate: Double) {
  private val formator: DecimalFormat = new DecimalFormat(".00%")
  override def toString: String = s"$cityName:${formator.format(rate)}"
}

