package date_related

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//Given a DataFrame with a date column, find the maximum and minimum dates in the dataset..
object Q6_min_max_date {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val df = List(   "2023-06-07",
                     "2023-10-10",
                     "2024-09-01"
                    ).toDF("date_str")

    val date = df.withColumn("date1",to_date($"date_str"))
     date.select(max($"date1").as("max_date"),min($"date1").as("min_date")).show()
    println("=========================================")
    val maxDate = date.select(max($"date1")).collect()(0)(0)
    val minDate = date.select(min($"date1")).collect()(0)(0)
    println(s"Maximum Date: $maxDate")
    println(s"Minimum Date: $minDate")


  }

}
