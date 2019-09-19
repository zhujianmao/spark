package com.atguigu.spark.accumulator

import com.atguigu.spark.bean.UserVisitAction
import com.atguigu.spark.constant.Constant
import org.apache.spark.util.AccumulatorV2

class CategoryTop10Accumulator extends AccumulatorV2[UserVisitAction, Map[(String, String), Long]] with Constant{
  var map: Map[(String, String), Long] = Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
    val acc: CategoryTop10Accumulator = new CategoryTop10Accumulator
    acc.map ++= map
    acc
  }

  override def reset(): Unit = map = Map[(String, String), Long]()

  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != -1){
      val clickKey: (String, String) = (v.click_category_id.toString,CLICK)
      map += clickKey -> (map.getOrElse(clickKey,0L) + 1L)
    }else if(v.order_category_ids != "null"){
      val split: Array[String] = v.order_category_ids.split(",")
      split.foreach(orderCategoryId =>{
        val orderKey: (String, String) = (orderCategoryId,ORDER)
        map += orderKey -> (map.getOrElse(orderKey,0L)+1L)
      })
    }else if(v.pay_category_ids != "null"){
      val split: Array[String] = v.pay_category_ids.split(",")
      split.foreach(payCategoryId=>{
        val payKey: (String, String) = (payCategoryId,PAY)
        map += payKey -> (map.getOrElse(payKey,0L)+1L)
      })
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
    val o: CategoryTop10Accumulator = other.asInstanceOf[CategoryTop10Accumulator]
    o.map.foreach{case (key,count)=> map += key -> (map.getOrElse(key,0L) + count)}
  }

  override def value: Map[(String, String), Long] = map
}
