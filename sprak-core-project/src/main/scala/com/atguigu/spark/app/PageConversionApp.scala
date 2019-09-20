package com.atguigu.spark.app

import java.text.DecimalFormat

import com.atguigu.spark.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object PageConversionApp {
  def calcuPageConversion(sc: SparkContext, userVisitActionRdd: RDD[UserVisitAction], pages:String): Unit = {
    //目标跳转流
    val splits: Array[String] = pages.split(",")
    //val postPage: Array[String] = splits.slice(1,splits.length)
    //val prePage: Array[String] = splits.slice(0,splits.length-1)
    // 1 2 3 4 5 6
    val prePage: Array[String] = splits.init
    // 2 3 4 5 6 7
    val postPage: Array[String] = splits.tail
    //跳转流   1->2,2->3,3->4,4->5,5->6,6->7
    val targetJump: Array[String] = prePage.zip(postPage).map{
      case (pre,post) => pre + "->"+post
    }
    val targetJumpbc: Broadcast[Array[String]] = sc.broadcast(targetJump)
    val splitsbc: Broadcast[Array[String]] = sc.broadcast(splits)
    //各个页面的点击率
  /*  val pageCount: collection.Map[Long, Long] =
                                userVisitActionRdd.filter(userVisitAction =>splits.contains(userVisitAction.page_id.toString)) //可以利用广播变量
                                                  .map(userVisitAction => (userVisitAction.page_id, 1))
                                                  .sortByKey()
                                                  .countByKey()*/

    val pageCount: collection.Map[Long, Long] =
                                userVisitActionRdd.filter(userVisitAction =>splitsbc.value.contains(userVisitAction.page_id.toString))
                                                  .map(userVisitAction => (userVisitAction.page_id, 1))
                                                  .sortByKey()
                                                  .countByKey()
    println("页面点击数:"+pageCount)
    //跳转数
    val targetJumpCount: collection.Map[String, Long] =
                                userVisitActionRdd.groupBy(_.session_id)
                                                  .flatMap {
                                                      case (_, it) =>
                                                            val allPageActions: List[UserVisitAction] = it.toList.sortBy(_.action_time)   //内存溢出?
                                                            val prePageAction: List[String] = allPageActions.slice(0, allPageActions.length - 1).map(_.page_id.toString)
                                                            val postPageAction: List[String] = allPageActions.slice(1, allPageActions.length).map(_.page_id.toString)
                                                            val allPageFlow: List[String] = prePageAction.zip(postPageAction).map {
                                                              case (pre, post) => pre + "->" + post
                                                            }
                                                           // allPageFlow.filter(targetJump.contains(_)).map((_, 1))  //可以利用广播变量减小内存的消耗
                                                            allPageFlow.filter(targetJumpbc.value.contains(_)).map((_, 1))
                                                  }.countByKey()

    println("跳转数:"+targetJumpCount)
    val result: collection.Map[String, String] = targetJumpCount.map {
      case (flow, count) =>
        val pageId: Long = flow.split("->")(0).toLong
        val rate: Double = count.toDouble / pageCount.getOrElse(pageId, Long.MaxValue)
        val formater: DecimalFormat = new DecimalFormat(".00%")
        (flow, formater.format(rate))
    }
    println("页面转化率:"+result)

  }
}
