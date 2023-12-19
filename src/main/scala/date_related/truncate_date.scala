package date_related

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//Calculate the number of days between a date column and the current date for
//each row in the DataFrame.
object truncate_date {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val df = List( ("2023-06-07",10),
                   ("2023-10-10", 15),
                   ("2024-09-01",20),
                   ("2023-10-10",5),
                   ("2023-06-07",20)
                    ).toDF("date_str","value")

//val agg_df = df.withColumn("date",to_date($"date_str"))
   val res = df.groupBy(trunc($"date_str","day")).agg(sum($"value").as("tot_value"))
    res.show()

  }
}
