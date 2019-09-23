package com.atguigu.sparksql

import org.apache.spark.sql.SparkSession

object SqlProject {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .appName("SqlProject")
      .getOrCreate()
    spark.udf.register("remark", new AreaRemarkUDAF)
    spark.sql("use sql_project")

    spark.sql(
      """
        |select
        |	area,city.city_name,prod.product_name
        |from user_visit_action user
        |join city_info city on  city.city_id = user.city_id
        |join product_info prod on prod.product_id = user.click_product_id
      """.stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |	area,product_name,count(1) prod_count,
        | remark(city_name) area_remark
        |from t1
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |	area,product_name,prod_count,
        |	rank() over(partition by area order by prod_count desc) rank,area_remark
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select
        |	area,product_name,area_remark
        |from t3
        |where rank <= 3
      """.stripMargin).show(100,truncate = false)


    spark.close()
  }
}
