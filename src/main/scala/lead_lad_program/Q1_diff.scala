package lead_lad_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, datediff, lag, lead}

//we want to find the difference between the price on each day with it’s previous day.
object Q1_diff {
  def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("Lead_Lag_Demo").master("local[*]").getOrCreate()
    import spark.implicits._
 val df = List(   (1,"KitKat",1000.0,"2021 - 01 - 01"),
                  (1,"KitKat",2000.0,"2021 - 01 - 02"),
                  (1,"KitKat",1000.0,"2021 - 01 - 03"),
                  (1,"KitKat",2000.0,"2021 - 01 - 04"),
                  (1,"KitKat",3000.0,"2021 - 01 - 05"),
                  (1,"KitKat",1000.0, "2021 - 01 - 06")
                   ).toDF("IT_ID", "IT_Name", "Price","PriceDate")

    val window_spe = Window.partitionBy("IT_Name").orderBy("PriceDate")

    val price_diff = df.withColumn("price_diffe.",col("Price") - lag("Price",1)
                                       .over(window_spe))

    price_diff.show()
     }

}
