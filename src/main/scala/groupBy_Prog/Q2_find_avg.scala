package groupBy_Prog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, desc, sum}
/*Finding the average score for each subject and the maximum score for each student. */
import org.apache.log4j.{Level, Logger}

//Find the average sales price for each product category and display the result in descending order of average price.
object Q2_find_avg {
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

     val avg_sale = data.groupBy("product").agg(avg("sales").as("avg_sale_price"))
                                           .orderBy(desc("avg_sale_price"))

    avg_sale.show()
  }
}
