package com.atguigu.sparkcore.day04.mysql

import java.sql.DriverManager

import org.apache.spark.SparkContext

//写数据
object MysqlWrite {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "MysqlWrite")

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop104:3306/mytest"
    val userName = "root"
    val passWd = "123456"
    val sql="insert into user values(?)"

    val rdd = sc.parallelize(Array(1,2,3,4))

    val result = rdd.foreachPartition(it => {
      Class.forName(driver)
      val conn = DriverManager.getConnection(url, userName, passWd)
      val stat = conn.prepareStatement(sql)
      for (elem <- it) {
        stat.setInt(1, elem)
        stat.addBatch()
      }
      stat.executeBatch()
      stat.close
      conn.close
    })
    sc.stop
  }
}
