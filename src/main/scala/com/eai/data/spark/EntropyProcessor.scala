package com.eai.data.spark

import com.eai.data.spark.EntropyUDFs._
import org.apache.spark.sql.functions.{collect_list, _}
import org.apache.spark.sql.types.{StringType, StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Processor for calculating entropy
  */
object EntropyProcessor {

  /**
    * Loads user-location data, transform and calculates entropy
    */
  def process(dataFile: String, headerString: String)(implicit spark: SparkSession): Unit = {

    val dataDF: DataFrame = loadDataDF(dataFile, headerString)
    val locationVisitsVectorDF: DataFrame = createLocationVisitsVectorDF(dataDF)
    val locationEntropyDF: DataFrame = locationEntropy(locationVisitsVectorDF)

    //Show output of process for now
    locationEntropyDF.printSchema()
    locationEntropyDF.show(false)
  }

  /**
    * Given data file location and header details
    * returns location and user (Visits by user) [[DataFrame]]
    */
  def loadDataDF(dataFile: String, headerString: String)(implicit spark: SparkSession): DataFrame = {
    val data = spark.sparkContext.textFile(dataFile)
      .map(_.split("\t"))
      .map(column => Row(column(0), column(1), column(2), column(3), column(4)))

    val fields = headerString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val dataDF = spark.createDataFrame(data, StructType(fields))
    dataDF
  }

  /**
    * Given location and user (Visits by user) [[DataFrame]]
    * returns locationLabel with locationVector for all users [[DataFrame]]
    */
  def createLocationVisitsVectorDF(inputDataDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val dataDF = inputDataDF
      .groupBy(col("location"), col("user")).count()
      .withColumn("userId", toInt(col("user"))).drop("user")
      .withColumn("visit", toDouble(col("count"))).drop("count")
      .orderBy(col("location"), col("userId"))

    val maxUserId = dataDF.agg(max(col("userId"))).head.getAs[Int](0) + 1

    dataDF
      .groupBy(col("location")).agg(collect_list("userId") as "userIds", collect_list("visit") as "locationVisits")
      .withColumn("locationVector", toVectorUDF(lit(maxUserId), col("userIds"), col("locationVisits")))
      .withColumnRenamed("location", "locationLabel")
      .select("locationLabel", "locationVector")
  }

  /**
    * Given locationLabel and locationVector (Number of of visits by users) [[DataFrame]]
    * returns locationLabel with locationEntropy [[DataFrame]]
    */
  def locationEntropy(locationVisitsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val visitsDFWithProbability = locationVisitsDF
      .withColumn("sum", vectorSumUDF(col("locationVector")))
      .withColumn("pLocationVector", probabilityVectorUDF(col("locationVector"), col("sum")))
      .withColumn("logOfpLocationVector", logOfVectorToBaseUDF(col("pLocationVector"), lit(2.0)))
      .withColumn("locationEntropy", entropyUDF(col("pLocationVector"), col("logOfpLocationVector")))

    visitsDFWithProbability.select("locationLabel", "locationEntropy")
  }

}
