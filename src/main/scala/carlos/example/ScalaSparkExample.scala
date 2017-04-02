package carlos.example

import org.apache.spark.sql.SparkSession

/**
  * Created by carlos on 4/1/17.
  */
class ScalaSparkExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Scala Spark Example")
      .getOrCreate()
    spark
  }
}
