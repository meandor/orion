package com.github.meandor

import java.time.{LocalDate, ZoneId}

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Features extends LazyLogging {

  def loadRawCSVFile(spark: SparkSession, csvFile: String): DataFrame = {
    val id = StructField("id", IntegerType, nullable = false)
    val date = StructField("date", StringType, nullable = false)
    val store_nbr = StructField("store_nbr", IntegerType, nullable = false)
    val item_nbr = StructField("item_nbr", IntegerType, nullable = false)
    val unit_sales = StructField("unit_sales", DoubleType, nullable = false)
    val onpromotion = StructField("onpromotion", StringType, nullable = false)
    val fields = Seq(id, date, store_nbr, item_nbr, unit_sales, onpromotion)
    val schema = StructType(fields)

    spark.read.format("csv").option("header", "true").schema(schema).load(csvFile)
  }

  def datasetWithTimestamp(spark: SparkSession, dataset: DataFrame, columnName: String): DataFrame = {
    val toTimestamp = (s: String) => {
      LocalDate.parse(s).atStartOfDay(ZoneId.systemDefault()).toEpochSecond
    }
    import org.apache.spark.sql.functions.udf
    val timeStampUDF = udf(toTimestamp)
    dataset.withColumn("timestamp", timeStampUDF(dataset.col(columnName))).drop(columnName)
  }

  def datasetWithBoolean(spark: SparkSession, dataset: DataFrame, columnName: String): DataFrame = {
    val toBoolean = (s: String) => {
      s match {
        case "1" => true
        case "true" => true
        case _ => false
      }
    }
    import org.apache.spark.sql.functions.udf
    val booleanUDF = udf(toBoolean)
    dataset.withColumn("withPromotion", booleanUDF(dataset.col(columnName))).drop(columnName)
  }

  def loadDataset(spark: SparkSession, csvFile: String): DataFrame = {
    logger.info("Reading csv file")

    val csvDataset = loadRawCSVFile(spark, csvFile)
    val withoutId = csvDataset.drop("id")
    val withTimestamp = datasetWithTimestamp(spark, withoutId, "date")
    val withBoolean = datasetWithBoolean(spark, withTimestamp, "onpromotion")
    val withLabel = withBoolean.withColumnRenamed("unit_sales", "label")

    val assembler = new VectorAssembler()
      .setInputCols(Array("timestamp", "item_nbr", "store_nbr", "withPromotion"))
      .setOutputCol("features")

    assembler.transform(withLabel).select("label", "features")
  }
}
