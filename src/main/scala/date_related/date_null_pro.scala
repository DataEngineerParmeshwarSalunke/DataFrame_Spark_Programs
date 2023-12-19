package date_related

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object date_null_pro {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val date_df = List(("2023-08-25",null),
                       (null,"2023-09-05"),
                       ("2023-10-01",null)).toDF("date1","date2")
    date_df.show()

    date_df.withColumn("new_date", coalesce(col("date1"),lit("found_nulls")))

      .withColumn("new_date2", when(col("date2").isNull,lit("2023-12-12"))
                    .otherwise(col("date2"))).show()

    val df = List(("2023-10-07", "15:30:00")).toDF("date", "time")

    df.withColumn("date_format", date_format($"date","dd MM yyyy"))
      .withColumn("time_format", date_format($"time","HH.mm.ss")).show()

    df.select(col("date"),  year($"date").as("year"),
                                     month($"date").as("month"),
                                     dayofmonth($"date").as("days"),
                                     dayofweek($"date").as("day_of_week"),
                                     dayofyear($"date").as("day-Of_year"),
                                     hour($"time").as("hour")
                                     ).show()

  }

}
