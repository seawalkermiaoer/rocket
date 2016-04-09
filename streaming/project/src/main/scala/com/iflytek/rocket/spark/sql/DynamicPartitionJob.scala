package com.iflytek.rocket.spark.sql

import com.iflytek.rocket.uitl.ConstValue
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

case class Opcode(
  bizid:String,
  osid:String,
  version:String,
  business:String,
  uid:String
)

/**
 * Created by seawalker on 2015/4/4.
 */
object DynamicPartitionJob {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("DynamicPartitionJob").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //load data
    val records = sc.textFile(ConstValue.DATA_PARENT_PATH + "/opcode.log")
      .map(_.split("\t")).map(x => Opcode(x(0), x(1),x(2),x(3),x(4)))

    //get partitions info
    val partitions = records.map(x => (x.bizid, x.osid, x.version, x.business)).distinct()

    val jobs = partitions.map(x => {
      val sql = String.format("select * from tmp where bizid = %s and osid = %s and version = %s and business =%s",
      x._1, x._2, x._3, x._4)
      val path =  String.format("/opcode/bizid=%s/osid=%s/version=%s/business=%s",
        x._1, x._2, x._3, x._4)
      (sql, path)
    })
    val opcodeDF = records.toDF()
    opcodeDF.registerTempTable("tmp")
    //opcodeDF.''/"
    jobs.foreach(println)
    //val test = sqlContext.sql("select * from tmp where bizid = 01 and osid = 01 and version = 511002 and business =22")
    //test.save(ConstValue.DATA_PARENT_PATH +  "/opcode/bizid=01/osid=01/version=511002/business=22", "parquet", SaveMode.Append)
    //partition stage
    for (job <- jobs.collect()){
      sqlContext.sql(job._1).save(ConstValue.DATA_PARENT_PATH + job._2, "parquet", SaveMode.Append)
    }

    while (true) {
      Thread.sleep(6000)
    }
  }

}
