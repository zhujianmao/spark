package com.atguigu.sparkcore.day03.myPartitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

object MyPartitioner {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext("local[2]","MyPartitioner")
    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c"), (4, "d")),2)
    val result: RDD[(Int, (Int, String))] = rdd.partitionBy(new MyPartitioner(3)).mapPartitionsWithIndex((index, it) => {
      it.map(kv => (index, kv))
    })
    result.collect.foreach(println)

    sc.stop
  }
}

class MyPartitioner(val numPartition:Int) extends Partitioner{
  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = {
    val k: Int = key.asInstanceOf[Int]
    (k % numPartition).abs
  }

  override def hashCode(): Int = super.hashCode()

  override def equals(obj: Any): Boolean = super.equals(obj)
}
