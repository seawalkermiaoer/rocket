package com.iflytek.rocket.spark.streaming


import com.iflytek.rocket.uitl.KafkaProducer
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import java.util.Calendar

import spray.json._
import DefaultJsonProtocol._

case class ViewTrace(user: String, action: String, productId: String, productType: String)

object ViewTraceProtocol extends DefaultJsonProtocol {
  implicit val viewTraceFormat = jsonFormat4(ViewTrace)
}


object Kafka {
  def main(args: Array[String]) {
    StreamingLogger.setStreamingLogLevels();
    //val Array(zkQuorum, group, topics, numThreads) = args
    val zkQuorum = "localhost:2181"
    val group = "test-group"
    val topics = "test"
    val numThreads = 2
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(" ").map((_,numThreads.toInt)).toMap
    val stream =  KafkaUtils.createStream(ssc, zkQuorum, group, topicMap);
    import ViewTraceProtocol._
    val traces = stream.map(_._2.parseJson.convertTo[ViewTrace])

    val pv = traces.map(x => (x.productType, 1)).reduceByKey(_ + _).map(
      x => (Calendar.getInstance().getTime().toString(), x._1, "PV", x._2)).map(_.toString())
    pv.foreachRDD( rdd =>
      rdd.foreachPartition(partitionOfRecords => {
        val producer = new KafkaProducer("stats", "localhost:9092")
        partitionOfRecords.foreach(record => producer.send(record.toString))
      })
    );

    val uv_grpd = traces.map(x => (x.productType, Set(x.user))).groupByKey();
    val uv = uv_grpd.foreachRDD(rdd => {
      rdd.filter(_._2.size > 0)
        .map(x => (Calendar.getInstance().getTime().toString(), x._1, "UV", x._2.size))
        .foreachPartition(partitionOfRecords => {
        val producer = new KafkaProducer("stats", "localhost:9092")
        partitionOfRecords.foreach(record => producer.send(record.toString))
      })
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}