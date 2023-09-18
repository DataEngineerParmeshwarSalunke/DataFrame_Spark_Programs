package dataframe_all_programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
//Given a DataFrame df containing sales data with columns product_id, quantity, and price,
//filter and display the rows where the quantity is greater than 10.
object Q2_filter_prodid_Quntity {
  def main(args:Array[String]): Unit={
    val spark = SparkSession.builder().appName("Salunke.com").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val df = List((10,10,5),(50,20,10),(101,0,8),(199,25,3),(205,5,15),(299,5,15),(0,5,15),(400,30,15))
                .toDF("product_id","quantity","price")

        df.show()
    println("display the rows where the quantity is greater than 10")
       df.filter(col("quantity")>10).show()

  }
}
