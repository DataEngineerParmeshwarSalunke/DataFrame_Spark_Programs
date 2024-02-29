package date_related

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.numericPrecedence.seq
import org.apache.spark.sql.functions._

object dateConvertIntoDate {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._
   val data =List(
     (1, "2024-02-01 00:00:00"),
     (2, "2024-02-01 00:00:00"),
     (3, "2024-02-03 00:00:00"),
     (4, "2024-02-04 00:00:00")).toDF("order_id","order_date")

    //Convert the string column to a timestamp
   val timestamp_format = "yyyy-MM-dd HH:mm:ss"
   val df_with_timestamp = data.withColumn("timestamp_col", unix_timestamp(col("Order_date"),
     timestamp_format).cast("timestamp"))

   val df_without_time = df_with_timestamp.withColumn("Order_date", date_format(col("timestamp_col"),
     "yyyy-MM-dd")).drop("timestamp_col")

    df_without_time.show()
    data.select(   col("order_id"), col("order_date"),
      to_date(col("order_date")).as("date")
    ).show()
  }
}
