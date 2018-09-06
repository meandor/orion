package com.github.meandor.orion

import org.apache.spark.sql.SparkSession

object Orion {

  val spark: SparkSession = SparkSession.builder.appName("Orion").getOrCreate()

  def main(args: Array[String]): Unit = {
    val trainDataset = spark.read.format("csv")
      .option("header", "true")
      .load("hdfs://ip-172-31-38-13.eu-central-1.compute.internal:8020/grocery/train.csv")

    trainDataset.show()
  }
}
