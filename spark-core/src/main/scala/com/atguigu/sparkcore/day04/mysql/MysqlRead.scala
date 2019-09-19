package com.atguigu.sparkcore.day04.mysql

import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

//读数据
object MysqlRead {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "MysqlWrite")
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop104:3306/mytest"
    val userName = "root"
    val passWd = "123456"
    val sql="select * from user where ? <= id and id <= ?"

    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      sql,
      1,
      10,
      2,
      result => result.getInt(1)
    )
    rdd.foreach(println)
    sc.stop
  }
}
