package windows_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc}
//Find the Ties in Sales Ranking within Each Category
object windows_find_ties {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder.appName("Windos_data").master("local[4]").getOrCreate()
      import spark.implicits._

    val df = List(("Electronics", "Laptop", 2000),
      ("Electronics", "Phone", 1500),
      ("Electronics", "Tablet", 2000),
      ("Clothing", "Shirt", 800),
      ("Clothing", "Jeans", 1000),
      ("Clothing", "Dress", 1000)
    ).toDF("Category", "Product", "Sales")

    val window = Window.partitionBy("Category").orderBy("Sales")
    df.withColumn("sample_rank", dense_rank().over(window))show()

    println("the Ties in Sales Ranking within Each Category")
  val ties =  df.withColumn("Rank", dense_rank().over(window)).filter(col("Rank")===1)
    ties.show()

    scala.io.StdIn.readLine()

  }
}
