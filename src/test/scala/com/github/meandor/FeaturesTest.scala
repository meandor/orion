package com.github.meandor

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{FeatureSpec, Matchers}

class FeaturesTest extends FeatureSpec with Matchers with LocalSparkContext {
  feature("load features from csv file") {
    scenario("csv file with inconsistent onpromotion values") {
      val trainDataset = Features.loadDataset(spark, "src/test/resources/train.csv")

      trainDataset.columns.toSeq shouldBe Seq("label", "features")
      trainDataset.head().toSeq shouldBe Seq(7.0, Vectors.dense(1.3569948E9, 103665, 25, 0))
    }
  }
}
