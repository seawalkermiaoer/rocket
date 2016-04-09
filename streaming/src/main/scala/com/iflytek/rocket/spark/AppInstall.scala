package com.iflytek.rocket.spark

import org.apache.spark._


/**
 * Created by seawalker on 2015/4/3.
 */

case class AppInstallInfo(
UID:String,
APPNAME : String,
APPPKG:String,
VERSION:String
)
object AppInstall {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("AppInstall").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    //110120104819452963~腾讯视频~com.tencent.qqlive~3.9.5.6890
    val records = sc.textFile("""G:\testdata\app\install.txt""").map(_.split("~", -1)).map(x => AppInstallInfo(x(0),x(1),x(2),x(3)))

//    val ret = records.map(x => ((x.APPNAME, x.APPPKG), 1)).reduceByKey(_ + _ ).coalesce(1).sortBy(x => x._2)
//      .map(x => Seq(x._1._1, x._1._2, x._2).mkString("\t"))

    val ret = records.map(x => (x.APPNAME, 1)).reduceByKey(_ + _ ).coalesce(1).sortBy(x => x._2)
      .map(x => Seq(x._1, x._2).mkString("\t"))

    ret.saveAsTextFile("""G:\testdata\app\name""")
    sc.stop()
  }
}
