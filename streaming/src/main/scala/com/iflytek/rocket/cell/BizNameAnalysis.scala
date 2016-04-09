package com.iflytek.rocket.cell

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by seawalker on 2015/4/17.
 */
object BizNameAnalysis {
  val DATA_BASE = """G:/testdata/cell"""

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("BizNameAnalysis").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

//    val MONEY_PATTERN = "[0-9]+元".r
//    val basic = sc.textFile(DATA_BASE + "/01").map(_.split("~", -1)).groupBy(_(0)).map(x => (x._1, x._2.map(_(2)).toList.mkString("~")))
//    val cnt = basic.map(x => (x._1,x._2, MONEY_PATTERN.findAllIn(x._2).size)).filter(_._3 == 0)
//    cnt.coalesce(4).saveAsTextFile(DATA_BASE + "/0元")

    val zero = sc.textFile(DATA_BASE + "/0元").map(_.split(",", -1)).map(x => (x(0),x(1).split("~").toSet))

    val ret = zero.groupBy(_._2).map(x => (x._2.size, x._1.mkString("~"))).coalesce(1).sortBy(_._1)
    println(ret.toDebugString)

    sc.stop()
  }
}
