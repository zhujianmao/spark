package com.atguigu.sparkcore.day04.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Write2Hbase {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Write2Hbase")

    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum",
      "hadoop102,hadoop103,hadoop104")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")

    // 通过job来设置输出的格式的类
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    val rdd: RDD[(String, String, String, String)] = sc.parallelize(Array(("104", "20", "lisi", "male")))

    val hbaseRDD: RDD[(ImmutableBytesWritable, Put)] = rdd.map(kv => {
      val put: Put = new Put(Bytes.toBytes(kv._1))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(kv._2))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(kv._3))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(kv._4))
      (new ImmutableBytesWritable(), put)
    })

    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop
  }
}
