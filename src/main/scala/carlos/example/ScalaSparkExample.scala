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

    val delim = ","
    // PURCH_ORDER,SUPPLIER_ORIGIN,CURRENCY
    val addInfo = sc.textFile("/Users/carlos/Downloads/DE - Technical Test CSVs/Additional_Info.csv").
      map(x => x.split(delim)).cache()//.filter(_.length != 3).map(x => (x(0),x(2)))
    val badAddInfo = addInfo.filter(_.length != 3).map(x => (x(0).trim(),"GBP")) // Based on country
    val goodAddInfo = addInfo.filter(_.length == 3).map(x => (x(0).trim(),x(2)))
    val fullAddInfo = badAddInfo.union(goodAddInfo).cache()
    fullAddInfo.count()
    addInfo.unpersist(true)

//    PURCH_ORDER,PART_NO,AREA,ORDER_TYPE,SUPPLIER,COST,CURRENCY,RAISED_DATE,CLOSED_DATE
    case class PartInfo(partNo: String, cost: Double, currency: String)
    val partCost = sc.textFile("/Users/carlos/Downloads/DE - Technical Test CSVs/Part_Costs.csv").
      map(_.split(delim)).filter(_.length != 0).map(x => (x(0).trim(),PartInfo(x(1),x(5).toDouble,x(6).trim))).cache()

    val partsWithCurrency = partCost.leftOuterJoin(fullAddInfo).map(_._2).map{
      case (partInfo, currency) => {
        (partInfo, currency.getOrElse("GBP"))
      }
    }
    partsWithCurrency.map(_._1).distinct().count() == partsWithCurrency.distinct().count() // no parts with dups

    partCost.leftOuterJoin(fullAddInfo).map(_._2).filter(_._2.isEmpty) // Not due to missing keys


    val compareCurrency = partsWithCurrency.filter(_._1.currency != "").map(x => (x._1.currency,x._2))
    compareCurrency.filter(x => x._1 != x._2) // Empty

    case class Cost(cost: Double, currency: String)
    val partNumToCost = partsWithCurrency.map(x => (x._1.partNo, Cost(x._1.cost, x._2)))


//    PART_NO,car
    val partToCar = sc.textFile("/Users/carlos/Downloads/DE - Technical Test CSVs/Part_Numbers.csv").
      map(_.split(delim)).map(x => (x(0).trim(),x(1)))

    val fixedPartToCar = partToCar.map{
      case (partNo,carNo)=> {
        var fixedCarNo = carNo.toLowerCase().
          replace("car","").replaceAll("[^\\w]","")
        fixedCarNo = fixedCarNo match {
          case "one" => "1"
          case "two" => "2"
          case "three" => "3"
          case "threee" => "3"
          case "four" => "4"
          case "five" => "5"
          case _ => fixedCarNo
        }
        (partNo,fixedCarNo)
      }
    }

    val carToPartCost = fixedPartToCar.join(partNumToCost).map(_._2) // Significant drop in rows, Probably OK
    val currencies = carToPartCost.map(_._2.currency).distinct()
    val currencyMap = sc.broadcast(Map(
      "CZK" -> .05,
      "AUD" -> .78,
      "GBP" -> 1.32,
      "SEK" -> .12,
      "EUR" -> 1.18,
      "USD" -> 1.0,
      "JPY" -> .0089,
      "ZAR" -> .073
    ))
    val carToUSD = carToPartCost.map{
      case(car,cost)=> {
        (car,(1,currencyMap.value(cost.currency)*cost.cost))
      }
    }
        import spark.sqlContext.implicits._
    val carCost = carToUSD.reduceByKey{
             case (v1, v2) => (v1._1+v2._1, v1._2+v2._2)
             }.map(_._2).map(x => x._2/x._1).toDF

//    val carCost = carToUSD.reduceByKey(_ + _).map(_._2).toDF
//
//    import org.apache.spark.sql.functions._
//    carCost.select(avg($"value")).show()

  }
  // Taken from internet
  def isAllDigits(x: String) = x forall Character.isDigit
}
