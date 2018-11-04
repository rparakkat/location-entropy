package com.eai.data.spark

import com.eai.data.{SparkMixin, UnitSpec}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}

class EntropyProcessorTest extends UnitSpec with SparkMixin {

  test("testLoadDataDF") {
    val dataDF = EntropyProcessor.loadDataDF("./src/test/resources/loc-test.txt",
      "user check-in-time latitude longitude location")(sparkSession).collect()
    assert(dataDF.length === 2)
    assert(dataDF(0).getAs[String]("user") === "0")
    assert(dataDF(0).getAs[String]("location") === "22847")
  }

  test("testLocationEntropy") {
    val locationVisitsDF = createLocationVisitsDF(sparkSession)
    val locationEntropy = EntropyProcessor.locationEntropy(locationVisitsDF)(sparkSession).collect()

    assert(locationEntropy.length === 2)
    assert(locationEntropy(0).getAs[String]("locationLabel") === "L1")
    assert(locationEntropy(0).getAs[Double]("locationEntropy") === 0.8166890883150209)
  }

  test("testCreateLocationVisitsVectorDF") {
    val inputDataDF = createInputDataDF(sparkSession)
    val locationVisitsVectorDF = EntropyProcessor.createLocationVisitsVectorDF(inputDataDF)(sparkSession).collect()
    assert(locationVisitsVectorDF.length === 2)
    assert(locationVisitsVectorDF(0).getAs[String]("locationLabel") === "L1")
    assert(locationVisitsVectorDF(0).getAs[DenseVector]("locationVector").values === Array(0.0, 1.0, 2.0))
  }

  def createInputDataDF(spark: SparkSession): DataFrame = {
    val data = Seq(
      ("L1", "1"), ("L1", "2"), ("L1", "2"),
      ("L2", "1"), ("L2", "1")
    )
    val inputDataDF = spark.createDataFrame(data).toDF("location", "user")
    inputDataDF
  }

  def createLocationVisitsDF(spark: SparkSession): DataFrame = {
    val locationVectorTuples = Seq(
      ("L1", Vectors.dense(10.0, 1.0, 1.0, 0.0)),
      ("L2", Vectors.dense(4.0, 4.0, 4.0, 0.0))
    )
    val locationVisitsDF = spark.createDataFrame(locationVectorTuples).toDF("locationLabel", "locationVector")
    locationVisitsDF
  }
}
