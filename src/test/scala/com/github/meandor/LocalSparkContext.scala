package com.github.meandor

import org.apache.spark.sql.SparkSession

trait LocalSparkContext {
  val spark: SparkSession = SparkSession.builder().appName("Orion").master("local[2]").getOrCreate()
}
