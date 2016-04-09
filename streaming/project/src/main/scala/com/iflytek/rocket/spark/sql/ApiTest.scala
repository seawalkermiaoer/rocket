package com.iflytek.rocket.spark.sql

import com.iflytek.rocket.uitl.ConstValue
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SaveMode, SQLContext}

case class Record(key: Int, value: String)
case class Person(name: String, age: Int)

/**
 * Created by seawalker on 2015/4/3.
 */
object ApiTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
//    val df = sc.parallelize((10 to 19).map(i => Record(i, s"val_$i"))).toDF()
//    df.registerTempTable("records")
//    //df.saveAsParquetFile(ConstValue.DATA_PARENT_PATH +  "/test/key=1",)
//    df.save(ConstValue.DATA_PARENT_PATH +  "/test/key=1", "parquet", SaveMode.Append)

        // Read in parquet file.  Parquet files are self-describing so the schmema is preserved.
    val parquetFile = sqlContext.parquetFile(ConstValue.DATA_PARENT_PATH +  "/opcode")
    parquetFile.registerTempTable("tmp")
    sqlContext.sql("select * from tmp where bizid=01 and version in (411002, 511002)")
      .collect()
      .foreach(println)
    sc.stop()
  }
}
