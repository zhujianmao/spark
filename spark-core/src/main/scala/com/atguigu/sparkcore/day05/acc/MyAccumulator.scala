package com.atguigu.sparkcore.day05.acc

import org.apache.spark.util.AccumulatorV2

class MyAccumulator extends AccumulatorV2[Long,Long]{
  var sum = 0L
  override def isZero: Boolean = sum == 0

  override def copy(): AccumulatorV2[Long, Long] = {
    val newAcc = new MyAccumulator
    newAcc.sum = this.sum
    newAcc
  }

  override def reset(): Unit = sum = 0

  override def add(v: Long): Unit = sum += v

  override def merge(other: AccumulatorV2[Long, Long]): Unit = {
    other match {
      case o:MyAccumulator => sum += o.sum
      case _ => throw new IllegalStateException
    }
  }

  override def value: Long = sum
}
