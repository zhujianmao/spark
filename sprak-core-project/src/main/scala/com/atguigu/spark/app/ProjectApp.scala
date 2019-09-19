package com.atguigu.spark.app

import com.atguigu.spark.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext("local[2]", this.getClass.getSimpleName.split("\\$")(0))

    val rdd: RDD[String] = sc.textFile("D:\\学习资料\\尚硅谷\\大数据阶段\\15_spark\\02_资料\\user_visit_action.txt")

    val userVisitActionRdd: RDD[UserVisitAction] = rdd.map(line => {
      val splits: Array[String] = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })
    //热门品类的top10
    val categoryTop10: List[CategoryCountInfo] = CategoryTop10App.categoryTop10(sc,userVisitActionRdd)
    //Top10热门品类中每个品类的 Top10 活跃 Session 统计
//    CategorySessionTop10App.statCategorySessionTop10(userVisitActionRdd,categoryTop10)
//    CategorySessionTop10App.statCategorySessionTop10_1(userVisitActionRdd,categoryTop10)
    CategorySessionTop10App.statCategorySessionTop10_2(userVisitActionRdd,categoryTop10)

    sc.stop
  }
}
