package com.iflytek.rocket.tools

import com.iflytek.rocket.uitl.ConstValue
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


/**
 * Created by seawalker on 2015/7/22.
 */
object ColumnRange {
  def main(args: Array[String]) {
    val lines = Source.fromFile(ConstValue.DATA_PARENT_PATH +  "sample.txt").getLines()

    val kvs = lines.map(_.split("\t")).map(x => {
      val bizid = x(0)
      val osid = x(1)
      val ver = x(2)
      val ctype = x(3)
      val cabstract = x(4)
      val pv = x(5).toInt
      val uv = x(6).toInt
      ((bizid, osid, cabstract.concat("|").concat(ctype), ver), (pv, uv))
    }).toSeq

    val sparkConf = new SparkConf().setAppName("ColumnRange").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val table = sc.parallelize(kvs).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => Seq(x._1._1, x._1._2, x._1._3, x._1._4, Seq(x._2._1, x._2._1).mkString("|")))

    //issue,version,tip
    val terms = table.filter(x =>
      x(0) == "108viafly" && x(2).startsWith("com.iflytek") && x(3) >= "311988" && x(3) < "312085")
      .map(_.slice(2, 5)).map(x => (x(0),x(1),x(2))).collect()

    val vers = terms.map(_._2).distinct.sorted
    val versWithIndex =  vers.map(x => (x, vers.indexOf(x))).toMap

    //val issues = terms.map(_._1).distinct.sorted
    val issues = terms.map(x => (x._1, x._2))
      .groupBy(_._1)
      .map(x=> (x._1, x._2.map(_._2).min))
      .toSeq.sortWith(_._2 > _._2).map(_._1).toSeq
    val issuesWithIndex = issues.map(x => (x, issues.indexOf(x))).toMap
    val sv = terms.map(x => (x._2, issuesWithIndex.get(x._1).get)).groupBy(_._1).map(x => {
      val version = x._1
      val low = versWithIndex.get(version).get
      val high = low + 1
      val data = x._2.map(_._2).toList.map(x => DataElement(x, low, high))
      SeriesVersion(version, "Tasks", data)
    })

    def convertDE2Json(x:DataElement): String ={
      "x:".concat(x.x.toString).concat(",low:").concat(x.low.toString).concat(",high:").concat(x.high.toString)
    }
    //
    val a = sv.toSeq.sortBy(x=> x.name).map(x => {
       val data = "data: [" + x.data.map(x => convertDE2Json(x)).map("{" + _ + "}").mkString(",") + "]"
      val name = "name:'" + x.name + "'"
      val stack = "stack:'" + x.stack + "'"
      "{" + Seq(name,stack, data).mkString(",") + "}"
    }).mkString(",\n")

    issues.map(x => "'".concat(x).concat("',")).foreach(println)


    println("\n")
    println(a)
  }
}

case class DataElement(x:Int,low:Int,high:Int)
case class SeriesVersion(name:String, stack:String, data:List[DataElement])




/**
 *    {
                    name: '4.0.1205',
                    stack: 'Tasks',
                    data: [{
                        x: 1,
                        low: 3,
                        high: 4
                    }]
                }
 */

