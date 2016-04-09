package com.iflytek.rocket.spark

import scala.io.Source

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by seawalker on 2015/4/6.
 */
object BasicTest {
  def main(args: Array[String]) {
    val m = Map( 1 -> "100ime", 2-> "android")
    val x = for ((k, v) <- m if k > 1) yield (k.getClass, v)
    //x.foreach(println)

    val a = Source.fromFile("G:/testdata/crash/dict.txt")
    val b = a.getLines().map(_.split(",")).map(x => (x(0), x(1)))
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val c = sc.parallelize(b.toList).groupBy(_._1)
    val d = c.flatMap(x => {
      val size = x._2.size
      val pairs = x._2.toList
      val ret = scala.collection.mutable.ArrayBuffer[(String,String,String)]()
      for (i <- 0 until size / 2){
        ret.insert(0, (x._1, pairs(i)._2, (100 + i).toString))
      }
      ret.toList
    })
    d.foreach(println)
  }
}
