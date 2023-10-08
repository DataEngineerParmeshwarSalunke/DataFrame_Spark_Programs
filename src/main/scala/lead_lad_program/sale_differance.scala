package lead_lad_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead}
//Calculate the difference in daily sales for each product
object sale_differance {
  def main(args:Array[String]):Unit={
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().appName("Lead_Lag_Demo").master("local[*]").getOrCreate()
    import spark.implicits._
 val df = List(   ("ProductA","CategoryX","2023-09-01",100.0 ),
                  ("ProductA","CategoryX","2023-09-02",120.0),
                  ("ProductA"," CategoryX","2023-09-03", 130.0),
                  ("ProductB","CategoryY","2023-09-01",200.0),
                  ("ProductB","CategoryY","2023-09-02", 210.0)
                  ).toDF("product", "category", "date","sales")
    df.show()
    val windo_spe = Window.partitionBy("category").orderBy("date")

    val sale_diff = df.withColumn("sale_diffrance", col("sales") - lag("sales",1)
      .over(windo_spe))
    sale_diff.show()
  }

}
