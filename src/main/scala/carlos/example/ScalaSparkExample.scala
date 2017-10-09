package carlos.example

import org.apache.spark.sql.SparkSession

/**
  * Created by carlos on 4/1/17.
  */
object ScalaSparkExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      master("local").
      appName("Scala Spark Example").
      getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    sc.parallelize(List("Hello","World!")).
      collect().
      foreach(println)

  }
}
