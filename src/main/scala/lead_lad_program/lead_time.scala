package lead_lad_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, datediff, lag, lead}

//Calculate the lead time for each order within the same customer
object lead_time {
  def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("Lead_Lag_Demo").master("local[*]").getOrCreate()
    import spark.implicits._
 val df = List(    (101,"CustomerA","2023-09-01"),
                   (103,"CustomerA","2023-09-03"),
                    (102,"CustomerB","2023-09-02"),
                   (104,"CustomerB","2023-09-04")
                   ).toDF("order_id", "customer", "order_date")

    val window_spe = Window.partitionBy("customer").orderBy("order_date")

    val lead_time = df.withColumn("lead_Time",col("order_date") - lead("order_date",1)
      .over(window_spe))
   // lead_time.show()
    val lead_time1 = df.withColumn("lead_Time", datediff(lead("order_date", 1)
                         .over(window_spe), col("order_date")))

    lead_time1.show()
     }

}
