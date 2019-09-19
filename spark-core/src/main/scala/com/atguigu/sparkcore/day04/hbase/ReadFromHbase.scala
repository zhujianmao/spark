package com.atguigu.sparkcore.day04.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ReadFromHbase {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Write2Hbase")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    conf.set(TableInputFormat.INPUT_TABLE, "student")

    val rdd = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val rdd2: RDD[String] = rdd.map {
      case (key, _) => Bytes.toString(key.get())
    }
    rdd2.collect.foreach(println)
    sc.stop
  }
}
