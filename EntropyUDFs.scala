package com.eai.data.spark

import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.math._

/**
  * Collection of User Defined functions that are used with the Spark [[org.apache.spark.sql.DataFrame]]
  */
object EntropyUDFs {

  val vectorSum: (DenseVector => Double) = { (vector) =>
    val size = vector.size

    var sum = 0.0
    var i = 0
    while (i < size) {
      sum += math.abs(vector(i))
      i += 1
    }
    sum
  }

  val vectorSumUDF: UserDefinedFunction = udf(vectorSum)

  val probabilityVector: ((DenseVector, Double) => DenseVector) = { (vector, sum) =>
    val size = vector.size
    val probabilityVector = new Array[Double](size)
    var i = 0
    while (i < size) {
      probabilityVector(i) = vector(i) / sum
      i += 1
    }
    new DenseVector(probabilityVector)
  }

  val probabilityVectorUDF: UserDefinedFunction = udf(probabilityVector)


  val logOfVectorToBase: ((DenseVector, Int) => DenseVector) = { (vector, base) =>
    val size = vector.size
    val logBasedVectorArray = new Array[Double](size)
    var i = 0
    while (i < size) {
      if (vector(i) != 0)
        logBasedVectorArray(i) = log(vector(i)) / log(base)
      else
        logBasedVectorArray(i) = 0.0
      i += 1
    }
    new DenseVector(logBasedVectorArray)
  }

  val logOfVectorToBaseUDF: UserDefinedFunction = udf(logOfVectorToBase)

  val entropy: ((DenseVector, DenseVector) => Double) = { (pVector, logOfpVector) =>
    val size = pVector.size
    var sum = 0.0
    var i = 0
    while (i < size) {
      sum += pVector(i) * logOfpVector(i)
      i += 1
    }
    -sum
  }

  val entropyUDF: UserDefinedFunction = udf(entropy)

  val toVector: ((Int, Seq[Int], Seq[Double]) => DenseVector) = { (size, userIds, locationVisits) =>
    Vectors.sparse(size, userIds.toArray, locationVisits.toArray).toDense
  }

  val toVectorUDF: UserDefinedFunction = udf(toVector)

  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)

  val toDouble: UserDefinedFunction = udf[Double, Int](_.toDouble)
}
