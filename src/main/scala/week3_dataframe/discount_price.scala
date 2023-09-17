package week3_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object discount_price {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession.builder().appName("DATAFRAME").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = Seq(("Product A", 100), ("Product B", 200), ("Product C", 150),("Product D",160))
             .toDF("product", "price")

   val result =  df.select(   col("product").as("Product_name"),
                 col("price") ,
           when(col("price") > 150, col("price")*0.9)
         .otherwise(col("price").as("discount_price"))
     )
    result.show()
  }

}
