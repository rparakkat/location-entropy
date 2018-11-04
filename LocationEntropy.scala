package com.eai.data.spark

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object LocationEntropy {

  val logger: Logger = Logger.getLogger(LocationEntropy.getClass)

  def main(args: Array[String]) {
    if (args.length != 2) {
      logger.error("Usage : LocationEntropy <Location data file> <Schema> ;\n" +
        "Example with available test data \n" +
        "LocationEntropy \"./src/main/resources/loc-gowalla_totalCheckins.txt.gz\" \"user check-in-time latitude longitude location\"")
      sys.error("Usage : LocationEntropy <Location data file> <Schema> ;\n" +
        "Example with available test data \n" +
        "LocationEntropy \"./src/main/resources/loc-gowalla_totalCheckins.txt.gz\" \"user check-in-time latitude longitude location\"")
      System.exit(1)
    } else {
      logger.info(s"Start time of the Job ${DateTime.now()}")
      logger.info("Spark Driver arguments - " + args.mkString(" | "))

      val sparkConf = new SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      val sparkSession = SparkSession
        .builder()
        .config(sparkConf)
        .appName("LocationEntropy_0.1")
        .master("local[*]")
        .getOrCreate()

      try {
        val dataFile = args(0)
        val headerString = args(1)
        EntropyProcessor.process(dataFile, headerString)(sparkSession)
      }
      catch {
        case e: Exception =>
          logger.error("EntropyProcessor process encountered error: ", e)
      }
    }
  }
}
