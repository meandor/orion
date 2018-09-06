import org.apache.spark.sql.SparkSession

object Orion {

  val spark: SparkSession = SparkSession.builder.appName("Orion").getOrCreate()

  def main(args: Array[String]): Unit = {
    val trainDataset = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("grocery/train.csv")

    trainDataset.show()
  }
}
