package date_related
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{add_months, col, date_add, date_sub, datediff, to_date, to_timestamp}
object date_demo {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("date_Demo").master("local[*]").getOrCreate()
    import spark.implicits._

    val date_df = List(("2023-08-25","15:30:00","2023-08-10")).toDF("date1","time_str","date2")

//    val formatted_df = date_df.withColumn("new_date", to_date($"date_str"))
//     .withColumn("new_time", to_timestamp($"time_str"))
//
//    formatted_df.show()
//    date_df.printSchema()
//    formatted_df.printSchema()

   val date_diff = date_df.withColumn("Date_Diff", datediff($"date1",$"date2"))
  //  date_diff.show()

    date_df.select(col("date1"), date_add($"date1",20)).show()
    date_df.select(col("date1"), date_sub($"date1",10)).show()

    date_df.select(col("date1"), add_months($"date1",3).as("added_month")).show()

  }

}
