package date_related

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//Given a DataFrame with a timestamp column, extract the day of the week for
//each timestamp and display it as a new column
object Q5_extract_day {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val df = List(   "2023-10-07 12:00:00",
                     "2023-10-10 15:30:00",
                     "2023-04-15 04:40:14").toDF("timestamp_str")

    val result = df.withColumn("timestamp", to_timestamp($"timestamp_str"))
                   .withColumn("days_of_week", dayofweek($"timestamp"))
                   .withColumn("days_of_week2",date_format($"timestamp","EEEE"))
                   .withColumn("new_col",date_format($"timestamp","EEE, MMM d, ''yy"))
                  .withColumn("new_col2",date_format($"timestamp","yyyyy.MMMMM.dd GGG hh:mm aaa"))
                    .withColumn("new_col3",date_format($"timestamp","h:mm a"))

                .withColumn("new_col4",date_format($"timestamp","hh 'o' 'clock' a, zzzz"))

    result.show(truncate = false)

  }

}   /*
+-------------------+-------------------+------------+-------------+----------------+----------------------------+--------+----------------------------------+
|timestamp_str      |timestamp          |days_of_week|days_of_week2|new_col         |new_col2                    |new_col3|new_col4                          |
+-------------------+-------------------+------------+-------------+----------------+----------------------------+--------+----------------------------------+
|2023-10-07 12:00:00|2023-10-07 12:00:00|7           |Saturday     |Sat, Oct 7, '23 |02023.October.07 AD 12:00 PM|12:00 PM|12 o clock PM, India Standard Time|
|2023-10-10 15:30:00|2023-10-10 15:30:00|3           |Tuesday      |Tue, Oct 10, '23|02023.October.10 AD 03:30 PM|3:30 PM |03 o clock PM, India Standard Time|
|2023-04-15 04:40:14|2023-04-15 04:40:14|7           |Saturday     |Sat, Apr 15, '23|02023.April.15 AD 04:40 AM  |4:40 AM |04 o clock AM, India Standard Time|
  +-------------------+-----
  */