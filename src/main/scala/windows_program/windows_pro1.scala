package windows_program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object windows_pro1 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder.appName("Windows_data").master("local[4]").getOrCreate()
      import spark.implicits._

//Find the Top 2 Products within Each Category
val df = List(
  ("Electronics", "Laptop", 2000),
  ("Electronics", "Mobile", 2000),
  ("Electronics", "Phone", 1500),
  ("Electronics", "Tablet", 1200),
  ("Clothing", "Shirt", 800),
  ("Clothing", "Jeans", 1000),
  ("Clothing", "Dress", 600)).toDF("Category", "Product", "SalesAmount")

    val windowSpec11 = Window.partitionBy("Category").orderBy(desc("SalesAmount"))
    df.withColumn("samp_rank", rank().over(windowSpec11)).show()

    val rank_df = df.withColumn("top_2_product", rank().over(windowSpec11)).filter(col("top_2_product") <=2)

      println("Top 2 Products within Each Category")
      rank_df.show()

//Calculate Rank of Students within Each Subject
  val data_df1 = List(("Math", "Alice", 90),
    ("Math", "Bob", 85),
    ("Math", "Carol", 92),
    ("Science", "Alice", 78),
    ("Science", "Bob", 88),
    ("Science", "Carol", 82)
    ).toDF("Subject", "Student", "Score")

    val  win_stud = Window.partitionBy("Subject").orderBy(desc("Score"))

    data_df1.withColumn("sampleRank", rank().over(win_stud)).show()

  val rank_df1 = data_df1.withColumn("Rank_student", rank().over(win_stud)).filter(col("Rank_student")<=2)

    println("Rank of Students within Each Subject")
    rank_df1.show()

    scala.io.StdIn.readLine()

  }
}
