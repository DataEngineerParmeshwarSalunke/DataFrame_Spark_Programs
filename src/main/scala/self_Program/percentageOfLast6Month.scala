package self_Program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, desc, sum, to_date}

object percentageOfLast6Month {
  def main(args:Array[String]): Unit ={
  Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("salunke.com").master("local[*]").getOrCreate()
    import spark.implicits._
    val product_data = List(
    (1, "iphone", "01-01-2023", 15000),
    (2, "samsung", "01-01-2023", 11000),
    (3, "oneplus", "01-01-2023", 11000),
    (1, "iphone", "01-02-2023", 13000),
    (2, "samsung", "01-02-2023", 11000),
    (3, "oneplus", "01-02-2023", 12000),
    (1, "iphone", "01-03-2023", 16000),
    (2, "samsung", "01-03-2023", 10000),
    (3, "oneplus", "01-03-2023", 11000),
    (1, "iphone", "01-04-2023", 17000),
    (2, "samsung", "01-04-2023", 18000),
    (3, "oneplus", "01-04-2023", 11700),
    (1, "iphone", "01-05-2023", 12000),
    (2, "samsung", "01-05-2023", 98000),
    (3, "oneplus", "01-05-2023", 11000),
    (1, "iphone", "01-06-2023", 11000),
    (2, "samsung", "01-06-2023", 1000),
    (3, "oneplus", "01-06-2023", 12000)  )

  val  schema1 = List("product_id", "product_name","sales_month","price")
    val data = spark.createDataFrame(spark.sparkContext.parallelize(product_data))
    val df = data.toDF(schema1:_*)
    df.show()
   df.groupBy("product_id","product_name").agg(count("price")).show()
    // Convert the sales_month column to a DateType
    val salesDataWithDate = df.withColumn("sales_date", to_date($"sales_month", "dd-MM-yyyy"))
    salesDataWithDate.show()
    // Calculate the percentage of sales for the last 6 months
    val windowSpec = Window.partitionBy("product_id","product_name").orderBy(desc("sales_date"))
                       .rangeBetween(Window.currentRow, 5)

//    val salesPercentage = salesDataWithDate
//      .withColumn("total_sales_last_6_months", sum("price"))
//      .withColumn("sales_percentage", $"price" / $"total_sales_last_6_months" * 100)
//     // .drop("sales_date", "total_sales_last_6_months")
//
//    salesPercentage.show()

  }
}
