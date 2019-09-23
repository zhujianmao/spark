package com.atguigu.spark.streaming.day01.receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyReceiver").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    ssc.receiverStream(new MyReceiver("hadoop102", 9999))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print

    ssc.start()
    ssc.awaitTermination()
  }
}

class MyReceiver(val host: String, val port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  override def onStart(): Unit = {
    new Thread() {
      override def run(): Unit = receiveData
    }.start()
  }

  def receiveData() = {
    try {
      val socket: Socket = new Socket(host, port)
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
      var line: String = reader.readLine()
      while (line != null) {
        store(line)
        line = reader.readLine()
      }
    } catch {
      case e => e.printStackTrace
    } finally {
      restart("重新连接socket")
    }

  }

  override def onStop(): Unit = ???
}