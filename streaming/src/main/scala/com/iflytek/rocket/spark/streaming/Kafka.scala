package com.iflytek.rocket.spark.streaming

import java.util.Properties
import scala.collection.mutable.SynchronizedQueue
import org.apache.spark.rdd.RDD
import com.iflytek.producer.KafkaProducer
import kafka.producer._
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

//    val cnt = stream.count();
//    cnt.foreachRDD(
//      rdd => rdd.foreach(println)
//    );

//    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[2]")
//    val ssc = new StreamingContext(sparkConf, Seconds(1))
//    val rddQueue = new SynchronizedQueue[RDD[String]]()
//    val stream = ssc.queueStream(rddQueue)

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
//    val s1 = """{"user":"f563991e-cacb-4bf1-a216-84a4dc7fbe1f","action":"view","productId":"20","productType":"Mounts"}"""
//    val s2 = """{"user":"f563991e-cacb-4bf1-a216-84a4dc7fbe1f","action":"view","productId":"14","productType":"Covers"}"""
//    // Create and push some RDDs into
//    val lines = List(s1, s2)
//    for (i <- 1 to 2) {
//      rddQueue += ssc.sparkContext.makeRDD(lines, 10)
//      Thread.sleep(2000)
//    }
//    ssc.stop()
    ssc.awaitTermination()
  }
}