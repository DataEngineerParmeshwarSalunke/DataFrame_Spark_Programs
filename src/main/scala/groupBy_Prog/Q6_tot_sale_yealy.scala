package groupBy_Prog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, sum}
/*Finding the average score for each subject and the maximum score for each student. */
import org.apache.log4j.{Level, Logger}

// :Calculate the total sales revenue for each product category and each year from the given DataFrame,
// assuming there is a "year" column in the DataFrame.

object Q6_tot_sale_yealy {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("OrderDataExample").master("local").getOrCreate()

    import spark.implicits._

    val data = List(
      (1, "ProductA", "Electronics", 1000.0,"2023"),
      (2, "ProductB", "Clothing", 500.0,"2022"),
      (3, "ProductC", "Electronics", 800.0,"2022"),
      (4, "ProductD", "Clothing", 300.0,"2023"),
      (5, "ProductE", "Electronics", 1200.0,"2022"),
      (6, "ProductF", "Clothing", 1000.0,"2022"),
      (7, "ProductG", "Electronics", 300.0,"2023"))
      .toDF("Id", "category", "product", "sales","year")
    data.groupBy("product","year").agg(max("sales")).show()

    val groupedDF  = data.groupBy("product","year")
    val result = groupedDF.agg(sum("sales").as("total_revenue"))
    result.show()

  }
}
