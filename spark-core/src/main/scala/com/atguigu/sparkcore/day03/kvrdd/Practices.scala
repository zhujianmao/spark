package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.SparkContext

/**
  * 时间戳，省份，城市，用户，广告，字段使用空格分割。
  * 1516609143867 6 7 64 16
  * 1516609143869 9 4 75 18
  * 1516609143869 1 7 87 12
  *
  * 统计出每一个省份城市广告被点击次数的 TOP3
  * (province,city,ads),1
  * (province,city,ads),count
  * (province,city),(ads,count)
  *
  */
object Practices {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Practices")
    val lines = sc.textFile("D:\\学习资料\\尚硅谷\\大数据阶段\\15_spark\\02_资料\\agent.log")
    //(province,city,ads),1
    val prodcityAdsOne = lines.map(line => {
      val splits = line.split(" ")
      ((splits(1), splits(2), splits(4)), 1)
    })
    //(province,city,ads),count
    val prodcityAdsCount = prodcityAdsOne.reduceByKey(_ + _)
    //(province,city),(ads,count)
    val result = prodcityAdsCount.map {
      case ((province, city, ads), count) => ((province, city), (ads, count))
    }.groupByKey.map {
      case (procity, it) =>
        (procity, it.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    }.sortByKey()
    result.collect.foreach(println)
    sc.stop
  }

}


