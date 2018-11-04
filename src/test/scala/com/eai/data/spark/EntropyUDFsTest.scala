package com.eai.data.spark

import com.eai.data.UnitSpec
import org.apache.spark.ml.linalg.DenseVector

class EntropyUDFsTest extends UnitSpec {

  test("testEntropy") {
    val pVector: DenseVector = new DenseVector(Array(10.0, 10.0, 1.0, 0.0))
    val logOfpVector: DenseVector = new DenseVector(Array(-0.1, -0.1, -0.1, 0.0))
    val entropy = EntropyUDFs.entropy(pVector, logOfpVector)
    assert(entropy === 2.1)
  }

  test("testToVector") {
    val indices: Seq[Int] = Seq[Int](1, 2, 3, 4)
    val values: Seq[Double] = Seq[Double](1.0, 2.0, 3.0, 4.0)
    val vector = EntropyUDFs.toVector(5, indices, values)
    assert(vector.size === 5)
    assert(vector.numNonzeros === 4)
  }

  test("testSumVector") {
    val vector: DenseVector = new DenseVector(Array(10.0, 10.0, 1.0, 0.0))
    val vectorSum = EntropyUDFs.vectorSum(vector)
    assert(vectorSum === 21.0)
  }

  test("testProbabilityVector") {
    val vector: DenseVector = new DenseVector(Array(10.0, 8.0, 4.0, 0.0))
    val probabilityVector = EntropyUDFs.probabilityVector(vector, 2.0)
    assert(probabilityVector.values === Array(5.0, 4.0, 2.0, 0.0))
  }

  test("testLogOfVectorToBase") {
    val vector: DenseVector = new DenseVector(Array(8.0, 4.0, 2.0, 0.0))
    val logOfVectorToBase = EntropyUDFs.logOfVectorToBase(vector, 2)
    assert(logOfVectorToBase.values === Array(3.0, 2.0, 1.0, 0.0))
  }
}
