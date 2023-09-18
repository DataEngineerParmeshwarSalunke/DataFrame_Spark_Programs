package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
//Given a DataFrame df containing event data with a column event_timestamp, filter and
//display the rows where the event_timestamp is on or after a specific date (e.g., "2023-01-
//01")
object Q7_flter_timestmp {
  def main(args:Array[String]): Unit={

    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List("2023-01-01 11:15:00","2022-0-11 01:30:00","2021-05-15 09:25:00","2024-11-22 05:20:00",
    "2018-06-10 04:10:00","2024-08-05 02:35:00").toDF("even_timestamp")
    val specificDate = "2023-01-01"
   val time_stamp = df.filter(col("even_timestamp") >= specificDate )
    time_stamp.show()
   }
}
