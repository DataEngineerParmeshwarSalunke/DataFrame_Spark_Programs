package groupBy_Prog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, max, sum}
/*Finding the average score for each subject and the maximum score for each student. */
import org.apache.log4j.{Level, Logger}

//Calculate the total sales revenue for each product category, and also find the category with the highest total sales.
object Q3_find_totSale {
  def main(args: Array[String]): Unit = {
   // Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("OrderDataExample").master("local").getOrCreate()

    import spark.implicits._

    val data = List(
      (1, "ProductA", "Electronics", 1000.0),
      (2, "ProductB", "Clothing", 500.0),
      (3, "ProductC", "Electronics", 800.0),
      (4, "ProductD", "Clothing", 300.0),
      (5, "ProductE", "Electronics", 1200.0))
      .toDF("Id", "product", "category", "sales")

     val tot_sale = data.groupBy("category").agg(sum("sales").as("Tot_sale"))


    tot_sale.show(1)
  }
}
