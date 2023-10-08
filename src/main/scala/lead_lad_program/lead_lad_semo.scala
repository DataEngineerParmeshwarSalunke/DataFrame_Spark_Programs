package lead_lad_program
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}
object lead_lad_semo {
  def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("Lead_Lag_Demo").master("local[*]").getOrCreate()
  val df = spark.read
    .format("csv")
    .option("header",true)
    .option("inferschema",true)
    .option("path","C:/Users/dell/Desktop/SparkInputfiles")
    .load()
    df.show()
    val window_spec = Window.partitionBy("product").orderBy("date")
     df.withColumn("lead_col",lead("revenue",1).over(window_spec)).show()
     df.withColumn("lag_col",lag("revenue",1).over(window_spec)).show()
    val df_lag_revenue = df.withColumn("New_lag_rev", col("revenue") + lag("revenue",1)
                                .over(window_spec))
    println("Lag function")
    df_lag_revenue.show()
  val df_lead_revenue = df.withColumn("New_lead_rev", col("revenue")-lead("revenue",1)
                               .over(window_spec) )
    println("Lead function")

    df_lead_revenue.show()
    // Calculate the differance between current day revenue and the revenue from previous day
  }

}
