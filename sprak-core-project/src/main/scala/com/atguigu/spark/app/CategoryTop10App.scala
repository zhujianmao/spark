package com.atguigu.spark.app

import com.atguigu.spark.accumulator.CategoryTop10Accumulator
import com.atguigu.spark.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable

object CategoryTop10App {

  def categoryTop10(sc:SparkContext,userVisitActionRdd: RDD[UserVisitAction]): List[CategoryCountInfo] ={
    //用累加器实现点击,下单,支付的数量汇总
    val acc: CategoryTop10Accumulator = new CategoryTop10Accumulator
    sc.register(acc)

    userVisitActionRdd.foreach(userVisitAction => acc.add(userVisitAction))
    //总的各个指标
    val allCount: Map[(String, String), Long] = acc.value
    //分组后(3,Map((3,pay) -> 1192, (3,click) -> 5975, (3,order) -> 1749))
    val allCategoryCountInfo: immutable.Iterable[CategoryCountInfo] = allCount.groupBy(_._1._1).map({
      case (cid, map) => CategoryCountInfo(
        cid.toLong,
        map.getOrElse((cid, "click"), 0L),
        map.getOrElse((cid, "order"), 0L),
        map.getOrElse((cid, "pay"), 0L))
    })
    val resultTop10: List[CategoryCountInfo] = allCategoryCountInfo.toList.sortBy(allCategoryCountInfo =>
      (allCategoryCountInfo.clickCount,
        allCategoryCountInfo.orderCount,
        allCategoryCountInfo.payCount))(Ordering.Tuple3(Ordering.Long.reverse,
      Ordering.Long.reverse, Ordering.Long.reverse)).take(10).sortBy(x => -x.categoryId)
     // resultTop10.foreach(println)
      resultTop10
  }

}
