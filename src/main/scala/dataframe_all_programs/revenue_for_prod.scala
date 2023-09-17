package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object revenue_for_prod {
  def main(args:Array[String]): Unit={
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List(   (0,10,5),
                     (2,20,10),
                     (3,0,8),
                     (4,25,3),
                     (5,5,15)).toDF("product_id","quantity","price")

    df.select(     col("product_id"),
                   col("quantity"),
                   col("price") ,
            when(col("product_id") >0 ,col(("quantity"))*col("price"))
           .when(col("product_id") ===0 , "-1")
           .otherwise(0).as("Revenue")
    ).show()
  }
}
