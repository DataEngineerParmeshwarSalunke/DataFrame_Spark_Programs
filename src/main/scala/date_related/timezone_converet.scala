package date_related

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object timezone_converet {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val df = List(("2023-10-07 12:00:00", "UTC"), ("2023-10-07 12:00:00", "America/New_York"))
                             .toDF("timestamp_str", "timezone")

    df.withColumn("converted_timezone", from_utc_timestamp($"timestamp_str",$"timezone")).show()
  }

}
