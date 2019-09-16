package com.atguigu.sparkcore.day02.partitionrdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDSortBy {

  implicit val ord:Ordering[User] = new Ordering[User] {
    override def compare(x: User, y: User): Int = x.age - y.age
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDDMap")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("a", "b", "c", "d", "e", "f"), 3)
    val rdd1 = rdd.sortBy(x => x, false)
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.parallelize(Array(User(10, "z"), User(10, "a"), User(40, "a"), User(20, "z")))
    //val rdd3 = rdd2.sortBy(user=>user.age)
    // val rdd3 = rdd2.sortBy({ user=>(user.age,user.name)},false)
    val rdd3 = rdd2.sortBy(user => user)
    println(rdd3.collect().mkString(","))


    sc.stop
  }
}

case class User(age: Int, name: String)
