package groupBy_Prog

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{SparkSession, functions}
/*Finding the average score for each subject and the maximum score for each student. */
import org.apache.log4j.{Level, Logger}
//Calculate the total sales revenue for each product category from the given DataFrame.
object tot_sale_revenue {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("OrderDataExample").master("local").getOrCreate()

    import spark.implicits._

    val data = List(
      (1, "ProductA", "Electronics", 1000.0),
      (2, "ProductB", "Clothing", 500.0),
      (3, "ProductC", "Electronics", 800.0),
      (4, "ProductD", "Clothing", 300.0),
      (5, "ProductE", "Electronics", 1200.0))
      .toDF("Id", "category", "product", "sales")

     val tot_sale = data.groupBy("product").agg(sum("sales").as("Tot_sale"))

    tot_sale.show()
  }
}
