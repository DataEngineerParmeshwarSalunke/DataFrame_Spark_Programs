package windows_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, max, sum}

object windos_parti1_rank {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder.appName("Windos_data").master("local[4]").getOrCreate()
      import spark.implicits._
    val ratingData = List(
      ("User1", "Movie1", 4.5),
      ("User1", "Movie2", 3.5),
      ("User1", "Movie3", 2.5),
      ("User1", "Movie4", 4.0),
      ("User1", "Movie5", 3.0),
      ("User1", "Movie6", 4.5),
      ("User2", "Movie1", 3.0),
      ("User2", "Movie2", 4.0),
      ("User2", "Movie3", 4.5),
      ("User2", "Movie4", 3.5),
      ("User2", "Movie5", 4.0),
      ("User2", "Movie6", 3.5)
    ).toDF("User", "Movies", "Rating")
    val windowSpec = Window.partitionBy( col("User")).orderBy(col("Movie"))
    val avgrating = ratingData.withColumn("Average rating", avg("Rating").over(windowSpec))
    avgrating.show()
//Finding the maximum revenue for each product category and the corresponding product.
    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180)
    ).toDF("Product", "Category", "Revenue")

    val windowSpec1 = Window.partitionBy( col("Category")).orderBy(col("Revenue"))
    val maximum = salesData.withColumn("maximum_revenue", max("Revenue").over(windowSpec1))
    maximum.show()
//Calculate Running salesAmountTotal for each region
   val sale_df = List( ("2023-01-01",	"East",1000),
     ("2023-01-02","East",1500),
     ("2023-01-03","East",2000),
     ("2023-01-01","West",800),
     ("2023-01-02","West",1200),
    ("2023-01-03","West",1600) ).toDF("Date","Region","SalesAmount")

    val window_df = Window.partitionBy(col("Region")).orderBy(col("SalesAmount"))

    val running_tot = sale_df.withColumn("Running_total", sum("SalesAmount").over(window_df))
    running_tot.show()

    scala.io.StdIn.readLine()

  }
}
