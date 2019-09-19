package com.atguigu.spark.app

import com.atguigu.spark.bean.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CategorySessionTop10App {
  /**
    * 可能发生OOM,是由于数据executor执行
    *
    * (cid,it((session,count)) groupByKey
    * <=(cid,(session,count))  map
    * <= (cid,session),count  reduceByKey
    * <=(cid,session),1
    *
    * @param userVisitActionRdd
    * @param categoryTop10
    * @return
    */
  def statCategorySessionTop10(userVisitActionRdd: RDD[UserVisitAction],
                               categoryTop10: List[CategoryCountInfo]) = {
    val top10CategoryIds: List[Long] = categoryTop10.map(category => {
      category.categoryId
    })

    val allUserVisitActionInTop10: RDD[UserVisitAction] = userVisitActionRdd.filter(x => {
      top10CategoryIds.contains(x.click_category_id)
    })
    // (cid,session),1)
    val userVisitActionOne: RDD[((Long, String), Long)] = allUserVisitActionInTop10.map(userVisitAction => {
      ((userVisitAction.click_category_id, userVisitAction.session_id), 1L)
    })
    // ((cid,session),count) => (cid,(session,count))  => (cid,it((session,count))
    val cidSessionByGroupBy: RDD[(Long, Iterable[(String, Long)])] = userVisitActionOne.reduceByKey(_ + _).map({
      case ((cid, session), count) => (cid, (session, count))
    }).groupByKey
    val result: RDD[(Long, List[(String, Long)])] = cidSessionByGroupBy.mapValues(it => {
      val cidSessionList: List[(String, Long)] = it.toList //在executor上执行,可能导致oom,内存溢出
      cidSessionList.sortBy(kv => kv._2)(Ordering.Long.reverse).take(10)
    }).sortByKey(ascending = false)
    result.collect.foreach(println)
  }

  /**
    * 按每一个cid进行一个job 的执行
    * 有shuffle阶段,进行落盘,可以防止OOM
    *
    * @param userVisitActionRdd
    * @param categoryTop10
    */
  def statCategorySessionTop10_1(userVisitActionRdd: RDD[UserVisitAction],
                                 categoryTop10: List[CategoryCountInfo]) = {
    val top10CategoryIds: List[Long] = categoryTop10.map(category => {
      category.categoryId
    })

    val allUserVisitActionInTop10: RDD[UserVisitAction] = userVisitActionRdd.filter(x => {
      top10CategoryIds.contains(x.click_category_id)
    })
    // (cid,session),1)
    val userVisitActionOne: RDD[((Long, String), Long)] = allUserVisitActionInTop10.map(userVisitAction => {
      ((userVisitAction.click_category_id, userVisitAction.session_id), 1L)
    })
    // ((cid,session),count) => (cid,(session,count))  => (cid,it((session,count))
    val cidSessionByGroupBy: RDD[(Long, Iterable[(String, Long)])] = userVisitActionOne.reduceByKey(_ + _).map({
      case ((cid, session), count) => (cid, (session, count))
    }).groupByKey

    top10CategoryIds.foreach(cid => {
      val oneCidRDD: RDD[(Long, Iterable[(String, Long)])] = cidSessionByGroupBy.filter(_._1 == cid)
      val resultCategorySessionTop10 = oneCidRDD.flatMap({
        case (_, it) => it
      }).sortBy(kv => kv._2, ascending = false) //sortBy有shuffle
        .take(10)
        .map({
          case (session, count) => CategorySession(cid, session, count)
        })
      resultCategorySessionTop10.foreach(println)
    })

  }

  /**
    * 将不同的cid放入不同的分区,====>自定义分区齐解决将相同的cid放入一个分区
    * 然后每个分区内进行排序
    *
    * @param userVisitActionRdd
    * @param categoryTop10
    */
  def statCategorySessionTop10_2(userVisitActionRdd: RDD[UserVisitAction],
                                 categoryTop10: List[CategoryCountInfo]) = {
    val top10CategoryIds: List[Long] = categoryTop10.map(category => {
      category.categoryId
    })

    val allUserVisitActionInTop10: RDD[UserVisitAction] = userVisitActionRdd.filter(x => {
      top10CategoryIds.contains(x.click_category_id)
    })
    // (cid,session),1)
    val userVisitActionOne: RDD[((Long, String), Long)] = allUserVisitActionInTop10.map(userVisitAction => {
      ((userVisitAction.click_category_id, userVisitAction.session_id), 1L)
    })
    // ((cid,session),count) => (cid,(session,count))  => (cid,it((session,count))
    val categorySessionRDD: RDD[CategorySession] =
      userVisitActionOne.reduceByKey(new CidPartitioner(top10CategoryIds), _ + _)
        .map({
          case ((cid, session), count) => CategorySession(cid,session,count)
        })

    val result = categorySessionRDD.mapPartitions(it => {
      var treeSet = new mutable.TreeSet[CategorySession]()
      for (categorySession <- it) {
        //treeSet.add(categorySession)
        treeSet += categorySession
        if (treeSet.size > 10)
          treeSet = treeSet.take(10)
      }
      treeSet.toIterator
    })
    result.collect.foreach(println)

  }

}

class CidPartitioner(top10CategoryIds: List[Long]) extends Partitioner {
  override def numPartitions: Int = top10CategoryIds.size
  val cidIndex: Map[Long, Int] = top10CategoryIds.zipWithIndex.toMap

  override def getPartition(key: Any): Int = {
    key match {
      case (cid:Long,_) => cidIndex(cid)
    }

  }
}
