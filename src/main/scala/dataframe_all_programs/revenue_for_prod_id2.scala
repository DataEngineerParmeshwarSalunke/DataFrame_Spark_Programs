package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object revenue_for_prod_id2 {
  def main(args:Array[String]): Unit={
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((10,10,5),(50,20,10),(101,0,8),(199,25,3),(205,5,15),(299,5,15),(0,5,15),(400,5,15))
                .toDF("product_id","quantity","price")

  df.select(     col("product_id"),
                   col("quantity"),
                   col("price") ,
   when(col("product_id") >= 1 && col("product_id") <= 100, col("quantity")*col("price"))
  .when(col("product_id") >= 101 && col("product_id") <= 200
                                  ,col("quantity")*col("price")*0.9 )
  .when(col("product_id") >= 201 && col("product_id") <= 300
                                  , col("quantity") * col("price") * 0.8)
  .otherwise("0").as("price on product_id")
    ).show()
  }
}
