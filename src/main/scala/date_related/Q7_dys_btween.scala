package date_related

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//Calculate the number of days between a date column and the current date for
//each row in the DataFrame.
object Q7_dys_btween {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val df = List(   "2023-06-07",
                     "2023-10-10",
                     "2024-09-01"
                    ).toDF("date_str")


    val date_diff = df.withColumn("date", to_date($"date_str"))
      .withColumn("date_gap", datediff(current_date(),$"date"))
    date_diff.show()
  }
}
