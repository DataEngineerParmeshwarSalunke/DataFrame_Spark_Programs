package groupBy_Prog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, quarter, sum, to_date}
/*Finding the average score for each subject and the maximum score for each student. */
import org.apache.log4j.{Level, Logger}

//  Calculate the total sales revenue for each product category and each quarter of the year from the given DataFrame,
//  assuming there is a "date" column containing dates in the format 'yyyy-MM-dd'.
object Q8_quarter_year {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("OrderDataExample").master("local").getOrCreate()
    import spark.implicits._

    val data = List(
      (1, "ProductA", "Electronics", 1000.0,"2023-03-30"),
      (2, "ProductB", "Clothing", 500.0,"2022-01-02"),
      (3, "ProductC", "Electronics", 800.0,"2022-01-24"),
      (4, "ProductD", "Clothing", 300.0,"2023-07-20"),
      (5, "ProductE", "Electronics", 1200.0,"2022-04-03"),
      (6, "ProductF", "Clothing", 1000.0,"2022-03-10"),
      (7, "ProductG", "Electronics", 300.0,"2023-09-15"))
      .toDF("Id", "category", "product", "sales","year")
       data.show()
    // Convert the "year" column to a date format
    val date_df = data.withColumn("new_date", to_date($"year"))

    // Extract the quarter from the date
    val quarter_df = date_df.withColumn("quarter", quarter($"new_date"))
    quarter_df.show()

    val result = quarter_df.groupBy("product","quarter").agg(sum("sales"))
      .orderBy("quarter")
    result.show()
  }
}
