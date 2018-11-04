package com.eai.data

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

trait SparkMixin extends TestSuiteMixin {
  this: TestSuite =>

  var sparkSession: SparkSession = _

  abstract override def withFixture(test: NoArgTest): Outcome = {
    sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try super.withFixture(test)
    finally sparkSession.stop()
  }

  def appID: String = (this.getClass.getName
    + math.floor(math.random * 10E4).toLong.toString)

  def conf: SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("Test")
      .set("spark.app.id", appID)
      .set("spark.ui.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.default.parallelism", "1")
  }
}
