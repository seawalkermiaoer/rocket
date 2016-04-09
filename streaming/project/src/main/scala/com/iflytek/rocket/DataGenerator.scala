package com.iflytek.rocket

import java.util.{Calendar, UUID}

/**
 * Created by seawalker on 2015/4/4.
 */
object DataGenerator {
  def main(args: Array[String]) {
    val bizid = Array("01","16")
    val osid = Array("01", "02")
    val version =  "511002~411002~300000~211000".split("~")
    val business = "22~24~12~45".split("~")

    for (i <- 0 until 10000) {
      val index1 = scala.util.Random.nextInt(2)
      val index2 = scala.util.Random.nextInt(4)
      val mostSigBits = scala.util.Random.nextInt
      val uid = new UUID(mostSigBits, Calendar.getInstance().getTimeInMillis)
      val record = Array(bizid(index1),osid(index1),version(index2),business(index2), uid)
      println(record.mkString("\t"))
      //Thread.sleep(1000)
    }

  }
}
