package com.atguigu.spark.streaming.day01.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDefinedSubmitOffset {

  val brokers: String = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
  val group: String = "bigdata"
  val topic: String = "test"
  val kafkaParams: Map[String, String] = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> group,
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
  )

  val kafkaCluster: KafkaCluster = new KafkaCluster(kafkaParams)
  //读取offset
  def readOffsets(topic:String): Map[TopicAndPartition, Long] = {
    val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
    var resultMap: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
    topicAndPartitionEither match {
        //主题在分区是否存在
      case Right(topicAndPartitionsSet) =>
              //topic在分区中存在
              val topicAndPartitonsWithOffsetsEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group,topicAndPartitionsSet)
              if (topicAndPartitonsWithOffsetsEither.isRight) {
                //不是第一次访问,读取offsets
                val topicPartitionWithOffsetsMap: Map[TopicAndPartition, Long] = topicAndPartitonsWithOffsetsEither.right.get
                resultMap ++= topicPartitionWithOffsetsMap
              }else{
                //第一次访问,offsets置为0
                topicAndPartitionsSet.foreach(topicAndPartition => {
                  resultMap += topicAndPartition -> 0L
                })
              }
          //主题在分区中不存在,返回空的   Map[TopicAndPartition, Long]()
      case _ =>
    }
    resultMap
  }

  def writeOffsets(sourceDstream: InputDStream[String])= {
    sourceDstream.foreachRDD(rdd =>{
      //将rdd强转为HasOffsetRanges
      val offSetsRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      val ranges: Array[OffsetRange] = offSetsRanges.offsetRanges
      var commitOffsetMap: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
      ranges.foreach(range=>{
        commitOffsetMap += range.topicAndPartition() -> range.untilOffset
      })
      kafkaCluster.setConsumerOffsets(group,commitOffsetMap)
    })
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaDefinedSubmitOffset").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val sourceDstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc,
      kafkaParams,
      readOffsets(topic),
      (mes: MessageAndMetadata[String, String]) => mes.message())

    val resultDstream: DStream[(String, Int)] = sourceDstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    resultDstream.print

    writeOffsets(sourceDstream)

    ssc.start()
    ssc.awaitTermination()
  }
}
