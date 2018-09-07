package com.github.meandor

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object Orion extends LazyLogging {

  val spark: SparkSession = SparkSession.builder.appName("Orion").getOrCreate()

  def main(args: Array[String]): Unit = {
    val trainDataset = Features.loadDataset(spark, "hdfs://ip-172-31-38-13.eu-central-1.compute.internal:8020/grocery/train.csv")

    logger.info("Writing transformed dataset")
    trainDataset.write.parquet("hdfs://ip-172-31-38-13.eu-central-1.compute.internal:8020/grocery/train.parquet")
  }
}
