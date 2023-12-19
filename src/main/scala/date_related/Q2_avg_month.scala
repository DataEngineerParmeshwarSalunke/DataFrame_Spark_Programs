package date_related

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//Given a DataFrame with date and value columns, calculate the average value
//for each month.
object Q2_avg_month {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val df = List(("2023-10-07", 10),
                 ("2023-10-07", 15),
                 ("2023-11-08", 20),
                 ("2023-10-20", 25),
                 ("2023-11-22", 5),
                 ("2024-09-03", 10))
                .toDF("date","value")
      df.withColumn("month", date_format($"date","yy/MM"))
        .groupBy("month")
        .agg(avg("value").as("avg_month_value")).show()
  }

}
