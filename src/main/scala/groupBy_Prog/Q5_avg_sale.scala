package groupBy_Prog

import org.apache.spark.sql.functions.{avg, max}
import org.apache.spark.sql.{SparkSession, functions}
/*Finding the average score for each subject and the maximum score for each student. */
import org.apache.log4j.{Level, Logger}

// Calculate the average sales price for each product category and add a new column to the DataFrame with the calculated average.

object Q5_avg_sale {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("OrderDataExample").master("local").getOrCreate()

    import spark.implicits._

    val data = List(
      (1, "ProductA", "Electronics", 1000.0),
      (2, "ProductB", "Clothing", 500.0),
      (3, "ProductC", "Electronics", 800.0),
      (4, "ProductD", "Clothing", 300.0),
      (5, "ProductE", "Electronics", 1200.0),
      (6, "ProductF", "Clothing", 1000.0),
      (7, "ProductG", "Electronics", 300.0))
      .toDF("Id", "category", "product", "sales")

    val avg_sale = data.groupBy("product").agg(avg("sales").as("calculated_avg"))

    avg_sale.show()

  }
}
