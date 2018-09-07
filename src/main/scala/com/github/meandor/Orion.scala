package com.github.meandor

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object Orion extends LazyLogging {

  val spark: SparkSession = SparkSession.builder.appName("Orion").getOrCreate()

  def main(args: Array[String]): Unit = {
    logger.info("Start reading dataset")
    val trainDataset = Features.loadDataset(spark, s"${HDFSHelper.basePath}/grocery/train.csv")
    logger.info("Done reading dataset")

    logger.info("Writing transformed dataset")
    val nextGeneration = HDFSHelper.nextGeneration(HDFSHelper.defaultHDFS(), "/grocery/dataset")
    trainDataset.write.parquet(s"${HDFSHelper.basePath}/grocery/dataset/$nextGeneration/train.parquet")
  }
}
