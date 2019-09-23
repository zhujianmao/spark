package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object WordCountCheckPoint {

  def createStreamingContext():StreamingContext={
    val conf: SparkConf = new SparkConf().setAppName("WordCountCheckPoint").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./chk1")
    val brokers: String = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic: String = "test"
    val group: String = "bigdata"
    val kafkaParams: Map[String, String] = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )
    // 泛型 key的类型,value的类型,key的解码器,value的解码器
    val kafkaDstream: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))

    kafkaDstream.print
    ssc
  }

  def main(args: Array[String]): Unit = {
    val scc: StreamingContext = StreamingContext.getActiveOrCreate("./chk1",createStreamingContext)

    scc.start()
    scc.awaitTermination()
  }
}
